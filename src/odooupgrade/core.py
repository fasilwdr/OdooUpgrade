import os
import shutil
import subprocess
import sys
import time
import zipfile
import logging
from typing import Optional, List

import requests
from packaging import version
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

console = Console()
logger = logging.getLogger("odooupgrade")


class OdooUpgrader:
    VALID_VERSIONS = ["10.0", "11.0", "12.0", "13.0", "14.0", "15.0", "16.0", "17.0", "18.0"]

    def __init__(self, source: str, target_version: str, verbose: bool = False, postgres_version: str = "13"):
        self.source = source
        self.target_version = target_version
        self.verbose = verbose
        self.postgres_version = postgres_version
        self.cwd = os.getcwd()
        self.source_dir = os.path.join(self.cwd, 'source')
        self.output_dir = os.path.join(self.cwd, 'output')
        self.filestore_dir = os.path.join(self.output_dir, 'filestore')
        self.compose_cmd = self._get_docker_compose_cmd()

    def _run_cmd(self, cmd: List[str], check: bool = True, capture_output: bool = False) -> subprocess.CompletedProcess:
        """Executes a subprocess command."""
        cmd_str = " ".join(cmd)
        logger.debug(f"Running command: {cmd_str}")

        try:
            result = subprocess.run(
                cmd,
                check=check,
                text=True,
                capture_output=capture_output
            )
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {cmd_str}")
            if capture_output and e.stderr:
                logger.error(f"Error output: {e.stderr.strip()}")
            raise

    def _get_docker_compose_cmd(self) -> List[str]:
        """Determines if 'docker compose' or 'docker-compose' is available."""
        try:
            subprocess.run(["docker", "compose", "version"], check=True, capture_output=True)
            return ["docker", "compose"]
        except (subprocess.CalledProcessError, FileNotFoundError):
            try:
                subprocess.run(["docker-compose", "--version"], check=True, capture_output=True)
                return ["docker-compose"]
            except (subprocess.CalledProcessError, FileNotFoundError):
                return ["docker", "compose"]

    def _cleanup_dir(self, path: str):
        """Safely removes a directory."""
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                logger.debug(f"Removed directory: {path}")
            except Exception as e:
                console.print(f"[yellow]Warning:[/yellow] Could not remove {path}: {e}")

    def validate_source_accessibility(self):
        """Checks if source file or URL is valid using Requests."""
        console.print("[blue]Validating source accessibility...[/blue]")

        if self.source.startswith("http://") or self.source.startswith("https://"):
            try:
                with requests.get(self.source, stream=True, timeout=15) as response:
                    response.raise_for_status()
                console.print("[green]Source URL is accessible.[/green]")
            except requests.RequestException as e:
                console.print(f"[bold red]Error:[/bold red] Source URL is not accessible: {e}")
                sys.exit(1)
        else:
            if not os.path.exists(self.source):
                console.print(f"[bold red]Error:[/bold red] Source file not found: {self.source}")
                sys.exit(1)
            console.print("[green]Source file exists.[/green]")

    def prepare_environment(self):
        """Creates necessary directories and cleans old data."""
        logger.info("Preparing environment...")
        self._cleanup_dir(self.source_dir)
        self._cleanup_dir(self.output_dir)

        os.makedirs(self.source_dir, exist_ok=True)
        os.makedirs(self.filestore_dir, exist_ok=True)

        if sys.platform != "win32":
            try:
                os.chmod(self.output_dir, 0o777)
                for root, dirs, files in os.walk(self.output_dir):
                    for d in dirs:
                        os.chmod(os.path.join(root, d), 0o777)
                    for f in files:
                        os.chmod(os.path.join(root, f), 0o777)
            except Exception as e:
                logger.warning(f"Could not set broad permissions on output dir: {e}")

    def download_or_copy_source(self) -> str:
        """Downloads file from URL using Requests with Rich progress bar."""
        target_path = ""

        if self.source.startswith("http://") or self.source.startswith("https://"):
            filename = os.path.basename(self.source.split("?")[0]) or "downloaded_db.dump"
            target_path = os.path.join(self.cwd, filename)

            try:
                with requests.get(self.source, stream=True) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("Content-Length", 0))

                    with Progress(
                            SpinnerColumn(),
                            TextColumn("[progress.description]{task.description}"),
                            BarColumn(),
                            TaskProgressColumn(),
                            "•",
                            TimeElapsedColumn(),
                            console=console
                    ) as progress:
                        task = progress.add_task("[cyan]Downloading source...", total=total_size)

                        with open(target_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))

            except requests.RequestException as e:
                console.print(f"[bold red]Download failed:[/bold red] {e}")
                sys.exit(1)
        else:
            target_path = self.source

        return target_path

    def process_source_file(self, filepath: str) -> str:
        """Extracts ZIP or prepares DUMP file."""
        ext = os.path.splitext(filepath)[1].lower()

        if ext == '.zip':
            console.print("[blue]Extracting ZIP file...[/blue]")
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(self.source_dir)
            return "ZIP"
        else:
            console.print("[blue]Processing DUMP file...[/blue]")
            shutil.copy2(filepath, os.path.join(self.source_dir, "database.dump"))
            return "DUMP"

    def create_db_compose_file(self):
        """Generates the docker-compose file for the database."""
        content = f"""
services:
  db-odooupgrade:
    container_name: db-odooupgrade
    image: postgres:{self.postgres_version}
    environment:
      - POSTGRES_DB=odoo
      - POSTGRES_PASSWORD=odoo
      - POSTGRES_USER=odoo
    networks:
      - odooupgrade-connection
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped

networks:
  odooupgrade-connection:
    driver: bridge
    name: odooupgrade-connection

volumes:
  postgres_data:
"""
        with open("db-composer.yml", "w", newline='\n') as f:
            f.write(content.strip())

    def wait_for_db(self):
        """Waits until Postgres is ready."""
        console.print("[yellow]Waiting for database to be ready...[/yellow]")
        max_retries = 30
        cmd = ["docker", "exec", "db-odooupgrade", "pg_isready", "-U", "odoo", "-d", "odoo"]

        for _ in range(max_retries):
            try:
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                console.print("[green]Database is ready.[/green]")
                return
            except subprocess.CalledProcessError:
                time.sleep(2)

        console.print("[bold red]Database failed to start.[/bold red]")
        sys.exit(1)

    def restore_database(self, file_type: str):
        """Restores the database dump."""
        console.print("[blue]Restoring database...[/blue]")

        self._run_cmd(["docker", "exec", "db-odooupgrade", "createdb", "-U", "odoo", "database"], check=False)

        if file_type == "ZIP":
            dump_path = os.path.join(self.source_dir, "dump.sql")
            if not os.path.exists(dump_path):
                found_sql = [f for f in os.listdir(self.source_dir) if f.endswith('.sql')]
                if found_sql:
                    dump_path = os.path.join(self.source_dir, found_sql[0])
                else:
                    console.print("[bold red]No dump.sql found inside ZIP.[/bold red]")
                    sys.exit(1)

            src_filestore = os.path.join(self.source_dir, "filestore")
            if os.path.exists(src_filestore):
                try:
                    shutil.copytree(src_filestore, self.filestore_dir, dirs_exist_ok=True)
                    if sys.platform != "win32":
                        try:
                            os.chmod(self.filestore_dir, 0o777)
                            for root, dirs, files in os.walk(self.filestore_dir):
                                for d in dirs:
                                    os.chmod(os.path.join(root, d), 0o777)
                                for f in files:
                                    os.chmod(os.path.join(root, f), 0o777)
                            logger.info("Filestore permissions corrected.")
                        except Exception as e:
                            logger.warning(f"Failed to set filestore permissions: {e}")
                except Exception as e:
                    logger.warning(f"Failed to copy filestore: {e}")

            self._run_cmd(["docker", "cp", dump_path, "db-odooupgrade:/tmp/dump.sql"])
            self._run_cmd(["docker", "exec", "-i", "db-odooupgrade", "psql", "-U", "odoo", "-d", "database", "-f",
                           "/tmp/dump.sql"], capture_output=True)

        elif file_type == "DUMP":
            dump_path = os.path.join(self.source_dir, "database.dump")
            self._run_cmd(["docker", "cp", dump_path, "db-odooupgrade:/tmp/database.dump"])

            cmd = [
                "docker", "exec", "db-odooupgrade", "pg_restore",
                "-U", "odoo", "-d", "database",
                "--no-owner", "--no-privileges", "--clean", "--if-exists",
                "--disable-triggers", "--single-transaction",
                "/tmp/database.dump"
            ]
            self._run_cmd(cmd, check=False)

    def get_current_version(self) -> str:
        """Queries the database to find the current Odoo version."""
        queries = [
            "SELECT latest_version FROM ir_module_module WHERE name = 'base' AND state = 'installed';",
            "SELECT value FROM ir_config_parameter WHERE key = 'database.latest_version';",
            "SELECT latest_version FROM ir_module_module WHERE name = 'base' ORDER BY id DESC LIMIT 1;"
        ]

        for q in queries:
            cmd = ["docker", "exec", "-i", "db-odooupgrade", "psql", "-U", "odoo", "-d", "database", "-t", "-A", "-c",
                   q]
            try:
                res = self._run_cmd(cmd, check=False, capture_output=True)
                ver = res.stdout.strip()
                if ver:
                    return ver
            except Exception:
                continue
        return ""

    def get_version_info(self, ver_str: str) -> version.Version:
        """Parses version string securely using packaging.version."""
        try:
            clean_ver = ver_str.strip()
            return version.parse(clean_ver)
        except Exception:
            return version.parse("0.0")

    def generate_next_version(self, current: str) -> str:
        """Calculates next major version (e.g. 15.0 -> 16.0)."""
        try:
            major = int(current.split('.')[0])
            return f"{major + 1}.0"
        except Exception:
            v = version.parse(current)
            return f"{v.major + 1}.0"

    def run_upgrade_step(self, target_version: str) -> bool:
        """Builds and runs the OpenUpgrade container."""
        dockerfile_content = f"""
FROM odoo:{target_version}
USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/OCA/OpenUpgrade.git --depth 1 --branch {target_version} /mnt/extra-addons
RUN pip3 install --no-cache-dir -r /mnt/extra-addons/requirements.txt
USER odoo
"""
        with open("Dockerfile", "w", newline='\n') as f:
            f.write(dockerfile_content.strip())

        compose_content = """
services:
  odoo-openupgrade:
    image: odoo-openupgrade
    build:
      context: .
      dockerfile: Dockerfile
    container_name: odoo-openupgrade
    environment:
      - HOST=db-odooupgrade
      - POSTGRES_USER=odoo
      - POSTGRES_PASSWORD=odoo
    networks:
      - odooupgrade-connection
    volumes:
      - ./output/filestore:/var/lib/odoo/filestore/database
      - ./output:/var/log/odoo
    restart: "no"
    entrypoint: /entrypoint.sh
    command: >
      odoo -d database
      --upgrade-path=/mnt/extra-addons/openupgrade_scripts/scripts
      --update all
      --stop-after-init
      --load=base,web,openupgrade_framework
      --log-level=info
      --logfile=/var/log/odoo/odoo.log
networks:
  odooupgrade-connection:
    external: true
    name: odooupgrade-connection
"""
        with open("odoo-upgrade-composer.yml", "w", newline='\n') as f:
            f.write(compose_content.strip())

        self._run_cmd(["docker", "rm", "-f", "odoo-openupgrade"], check=False, capture_output=True)

        cmd_up = self.compose_cmd + ["-f", "odoo-upgrade-composer.yml", "up", "--build", "--abort-on-container-exit"]

        with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console
        ) as progress:
            task = progress.add_task(f"[bold magenta]Upgrading to {target_version}...", total=None)

            try:
                process = subprocess.Popen(
                    cmd_up,
                    stdout=subprocess.DEVNULL if not self.verbose else None,
                    stderr=subprocess.PIPE if not self.verbose else None,
                    text=True
                )

                while process.poll() is None:
                    time.sleep(0.5)

                if process.returncode != 0:
                    console.print("[bold red]Upgrade process failed.[/bold red]")
                    if not self.verbose and process.stderr:
                        console.print(process.stderr.read())
                    return False
            except Exception as e:
                console.print(f"[bold red]Error running upgrade:[/bold red] {e}")
                return False

        try:
            res = self._run_cmd(["docker", "inspect", "odoo-openupgrade", "--format={{.State.ExitCode}}"],
                                capture_output=True)
            exit_code = int(res.stdout.strip())
            if exit_code == 0:
                console.print(f"[green]Upgrade to {target_version} successful.[/green]")
                self._run_cmd(self.compose_cmd + ["-f", "odoo-upgrade-composer.yml", "down"], check=False)
                return True
            else:
                console.print(f"[bold red]Container exited with code {exit_code}[/bold red]")
                return False
        except Exception:
            return False

    def finalize_package(self):
        """Dumps final database and zips it."""
        console.print("[blue]Creating final package...[/blue]")

        dump_cmd = ["docker", "exec", "db-odooupgrade", "pg_dump", "-U", "odoo", "database"]
        with open(os.path.join(self.output_dir, "dump.sql"), "w") as f:
            subprocess.run(dump_cmd, stdout=f, check=True)

        zip_name = os.path.join(self.output_dir, "upgraded.zip")
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(os.path.join(self.output_dir, "dump.sql"), "dump.sql")

            if os.path.exists(self.filestore_dir):
                for root, _, files in os.walk(self.filestore_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.output_dir)
                        zipf.write(file_path, arcname)

        console.print(f"[bold green]Upgrade Complete! Package available at: {zip_name}[/bold green]")
        os.remove(os.path.join(self.output_dir, "dump.sql"))

    def cleanup_artifacts(self):
        """Removes source folder and extracted filestore."""
        self._cleanup_dir(self.source_dir)
        self._cleanup_dir(self.filestore_dir)

    def cleanup(self):
        """Docker cleanup."""
        console.print("[dim]Cleaning up Docker environment...[/dim]")
        if os.path.exists("db-composer.yml"):
            self._run_cmd(self.compose_cmd + ["-f", "db-composer.yml", "down", "-v"], check=False)

        for f in ["Dockerfile", "odoo-upgrade-composer.yml", "db-composer.yml"]:
            if os.path.exists(f):
                os.remove(f)

    def run(self):
        try:
            if self.target_version not in self.VALID_VERSIONS:
                console.print(f"[bold red]Invalid version. Supported: {self.VALID_VERSIONS}[/bold red]")
                sys.exit(1)

            self.validate_source_accessibility()
            self.prepare_environment()
            self.create_db_compose_file()

            self._run_cmd(self.compose_cmd + ["-f", "db-composer.yml", "up", "-d"])
            self.wait_for_db()

            local_source = self.download_or_copy_source()
            file_type = self.process_source_file(local_source)

            if local_source != self.source and os.path.exists(local_source):
                os.remove(local_source)

            self.restore_database(file_type)

            current_ver_str = self.get_current_version()
            if not current_ver_str:
                console.print("[bold red]Could not determine database version.[/bold red]")
                sys.exit(1)

            console.print(f"[bold blue]Current Database Version: {current_ver_str}[/bold blue]")

            current_ver = self.get_version_info(current_ver_str)
            target_ver = self.get_version_info(self.target_version)
            min_ver = self.get_version_info("10.0")

            if current_ver < min_ver:
                console.print("[bold red]Source database version is below 10.0. Not supported.[/bold red]")
                sys.exit(1)

            while True:
                current_ver = self.get_version_info(current_ver_str)

                if current_ver.major == target_ver.major:
                    console.print("[green]Target version reached![/green]")
                    self.finalize_package()
                    self.cleanup_artifacts()
                    break
                elif current_ver.major > target_ver.major:
                    console.print("[yellow]Current version is already higher than target.[/yellow]")
                    self.finalize_package()
                    self.cleanup_artifacts()
                    break
                else:
                    next_ver_str = self.generate_next_version(current_ver_str)

                    if not self.run_upgrade_step(next_ver_str):
                        console.print("[bold red]Aborting sequence.[/bold red]")
                        sys.exit(1)

                    current_ver_str = self.get_current_version()
                    console.print(f"[blue]Database is now at version: {current_ver_str}[/blue]")

        except KeyboardInterrupt:
            console.print("[bold red]Operation cancelled by user.[/bold red]")
        except Exception as e:
            console.print(f"[bold red]Unexpected error:[/bold red] {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()