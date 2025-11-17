"""
Odoo Database Upgrade Tool - Python Version
Complete implementation matching the bash script functionality
"""

import docker
import click
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn
from typing import Optional
import logging
import time
import zipfile
import shutil
import re
import subprocess

console = Console()

class OdooUpgrader:
    """Main Odoo upgrade orchestrator"""

    def __init__(self, source: str, target_version: str, verbose: bool = False):
        self.source = source
        self.target_version = target_version
        self.verbose = verbose
        self.docker_client = docker.from_env()
        self.logger = self._setup_logging()
        self.work_dir = Path('./source')
        self.output_dir = Path('./output')

    def _setup_logging(self) -> logging.Logger:
        """Setup cross-platform logging"""
        logging.basicConfig(
            level=logging.DEBUG if self.verbose else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def check_prerequisites(self) -> bool:
        """Check if Docker is available and running"""
        try:
            self.docker_client.ping()
            console.print("[green]✓[/green] Docker is running")
            return True
        except docker.errors.DockerException as e:
            console.print(f"[red]✗[/red] Docker is not available: {e}")
            return False

    def download_or_load_source(self) -> Path:
        """Handle both local files and remote URLs"""
        if self.source.startswith(('http://', 'https://')):
            return self._download_file(self.source)
        else:
            path = Path(self.source)
            if not path.exists():
                raise FileNotFoundError(f"Source file not found: {self.source}")
            return path

    def _download_file(self, url: str) -> Path:
        """Download file with progress bar"""
        import requests

        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))

        # Extract filename from URL
        filename = url.split('/')[-1].split('?')[0]
        if not filename or filename == '/':
            filename = 'downloaded_file.zip'

        filepath = Path(filename)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TransferSpeedColumn(),
        ) as progress:
            task = progress.add_task("Downloading...", total=total_size)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    progress.update(task, advance=len(chunk))

        return filepath

    def extract_source(self, source_file: Path) -> tuple[Path, str]:
        """Extract source file and determine type"""
        self.work_dir.mkdir(exist_ok=True, parents=True)

        if source_file.suffix == '.zip':
            # Extract ZIP file
            with zipfile.ZipFile(source_file, 'r') as zip_ref:
                zip_ref.extractall(self.work_dir)

            # Look for dump.sql or database.dump
            dump_sql = self.work_dir / 'dump.sql'
            if dump_sql.exists():
                return dump_sql, 'sql'

            # Look for any .dump file
            dump_files = list(self.work_dir.glob('*.dump'))
            if dump_files:
                return dump_files[0], 'dump'

            raise FileNotFoundError("No dump.sql or .dump file found in ZIP")

        elif source_file.suffix == '.dump':
            # Copy dump file to work directory
            target = self.work_dir / 'database.dump'
            shutil.copy(source_file, target)
            return target, 'dump'

        else:
            raise ValueError(f"Unsupported file type: {source_file.suffix}")

    def create_database_container(self) -> docker.models.containers.Container:
        """Create PostgreSQL container using Docker SDK"""
        try:
            # Remove existing container if present
            try:
                old_container = self.docker_client.containers.get('db-odooupgrade')
                old_container.remove(force=True)
            except docker.errors.NotFound:
                pass

            # Create or get the network
            try:
                network = self.docker_client.networks.get('odooupgrade-connection')
            except docker.errors.NotFound:
                network = self.docker_client.networks.create(
                    'odooupgrade-connection',
                    driver='bridge'
                )

            # Create new container and connect to network
            container = self.docker_client.containers.run(
                image='postgres:15',
                name='db-odooupgrade',
                environment={
                    'POSTGRES_USER': 'odoo',
                    'POSTGRES_PASSWORD': 'odoo',
                    'POSTGRES_DB': 'odoo'
                },
                network='odooupgrade-connection',
                detach=True,
                remove=False
            )

            console.print("[green]✓[/green] Database container created")
            return container

        except docker.errors.DockerException as e:
            self.logger.error(f"Failed to create container: {e}")
            raise

    def restore_database(self, container: docker.models.containers.Container,
                        dump_file: Path, file_type: str) -> bool:
        """Restore database from dump file using docker cp like bash script"""
        spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        spinner_index = 0

        try:
            # Wait for PostgreSQL to be ready
            max_attempts = 30
            for attempt in range(max_attempts):
                exit_code, _ = container.exec_run(
                    'pg_isready -U odoo -d odoo'
                )
                if exit_code == 0:
                    break
                console.print(f"\r{spinner_chars[spinner_index % len(spinner_chars)]} Waiting for database...", end="")
                spinner_index += 1
                time.sleep(1)
            else:
                console.print("\n[red]✗[/red] Database not ready")
                return False

            console.print("\r" + " " * 50, end="\r")  # Clear line

            # Wait a bit more to ensure postgres is fully ready
            time.sleep(2)

            # Create database
            exit_code, output = container.exec_run('createdb -U odoo database')
            if exit_code != 0 and b'already exists' not in output:
                self.logger.error(f"Failed to create database: {output.decode()}")
                return False

            # Copy dump file to container using docker cp
            container_path = f'/tmp/{dump_file.name}'
            try:
                subprocess.run(
                    ['docker', 'cp', str(dump_file.absolute()), f'db-odooupgrade:{container_path}'],
                    check=True,
                    capture_output=True
                )
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to copy file to container: {e.stderr.decode()}")
                return False

            # Restore database with spinner
            console.print(f"{spinner_chars[0]} Restoring database...", end="")
            spinner_index = 0

            if file_type == 'sql':
                # Restore from SQL file
                exit_code, output = container.exec_run(
                    f'psql -U odoo -d database -f {container_path}',
                    demux=False
                )
            else:  # dump
                # Restore from pg_dump file
                exit_code, output = container.exec_run(
                    f'pg_restore -U odoo -d database --verbose --no-owner '
                    f'--no-privileges --clean --if-exists --disable-triggers '
                    f'--single-transaction {container_path}',
                    demux=False
                )

            console.print("\r" + " " * 50, end="\r")  # Clear line

            # pg_restore might return non-zero but still succeed
            if file_type == 'dump' or exit_code == 0:
                console.print(f"[green]✓[/green] Database restored successfully")

                # Verify restoration
                if self.verify_database_restoration(container):
                    return True
                else:
                    self.logger.error("Database verification failed")
                    return False
            else:
                self.logger.error(f"Restore failed: {output.decode()}")
                return False

        except Exception as e:
            self.logger.error(f"Restoration error: {e}")
            console.print(f"\n[red]✗[/red] Database restoration failed: {e}")
            return False

    def verify_database_restoration(self, container: docker.models.containers.Container) -> bool:
        """Verify database restoration"""
        try:
            # Check essential tables
            exit_code, output = container.exec_run(
                "psql -U odoo -d database -tAc "
                "\"SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name IN ('ir_module_module', 'res_users', 'ir_model');\""
            )

            if exit_code == 0:
                count = output.decode().strip()
                if count == '3':
                    return True
                else:
                    self.logger.error(f"Essential tables missing (found {count}/3)")
                    return False

            return False
        except Exception as e:
            self.logger.error(f"Verification error: {e}")
            return False

    def get_current_version(self, container: docker.models.containers.Container) -> str:
        """Get current Odoo version from database - tries multiple queries like bash script"""

        queries = [
            "SELECT latest_version FROM ir_module_module WHERE name = 'base' AND state = 'installed';",
            "SELECT value FROM ir_config_parameter WHERE key = 'database.latest_version';",
            "SELECT latest_version FROM ir_module_module WHERE name = 'base' ORDER BY id DESC LIMIT 1;",
        ]

        for query in queries:
            try:
                exit_code, output = container.exec_run(
                    f"psql -U odoo -d database -tAc \"{query}\""
                )

                if exit_code == 0:
                    version = output.decode().strip()
                    # Clean up the version string
                    version = re.sub(r'\s+', '', version)
                    version = re.sub(r'\|.*$', '', version)

                    if version and version != '':
                        self.logger.debug(f"Found version using query: {query[:50]}...")
                        return version
            except Exception as e:
                self.logger.debug(f"Query failed: {e}")
                continue

        raise ValueError("Could not retrieve version from database")

    def upgrade_to_version(self, container: docker.models.containers.Container,
                          current: str, target: str) -> bool:
        """Upgrade database from current to target version"""
        console.print(f"[yellow]Upgrading {current} → {target}[/yellow]")

        try:
            # Clean up any existing upgrade containers
            try:
                old_upgrade = self.docker_client.containers.get('odoo-openupgrade')
                old_upgrade.remove(force=True)
            except docker.errors.NotFound:
                pass

            # Create Dockerfile with proper file handling
            dockerfile_content = f"""FROM odoo:{target}
USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/OCA/OpenUpgrade.git --depth 1 --branch {target} /tmp/openupgrade && \
    cp -r /tmp/openupgrade/. /mnt/extra-addons/ && \
    rm -rf /tmp/openupgrade && \
    chown -R odoo:odoo /mnt/extra-addons
RUN ls -la /mnt/extra-addons/ && echo "--- Files in extra-addons ---" && ls /mnt/extra-addons/
RUN if [ -f /mnt/extra-addons/requirements.txt ]; then pip3 install --no-cache-dir -r /mnt/extra-addons/requirements.txt || true; fi
USER odoo
"""

            dockerfile_path = Path('./Dockerfile')
            dockerfile_path.write_text(dockerfile_content)

            # Build image
            console.print("Building upgrade image...")

            try:
                image, build_logs = self.docker_client.images.build(
                    path='.',
                    dockerfile='Dockerfile',
                    tag=f'odoo-openupgrade:{target}',
                    rm=True,
                    nocache=True,  # Force fresh build
                    forcerm=True
                )

                # Display logs if verbose
                if self.verbose:
                    for log in build_logs:
                        if 'stream' in log:
                            console.print(log['stream'].strip())
                        elif 'error' in log:
                            console.print(f"[red]Error: {log['error']}[/red]")

            except docker.errors.BuildError as e:
                console.print(f"[red]Build had errors[/red]")
                if self.verbose:
                    console.print("\n[yellow]Last 30 lines of build log:[/yellow]")
                    for log in e.build_log[-30:]:
                        if 'stream' in log:
                            console.print(log['stream'].strip())
                        elif 'error' in log:
                            console.print(f"[red]{log['error']}[/red]")

                # The image might still exist even if build failed
                console.print("[yellow]Checking if image was created despite errors...[/yellow]")
                try:
                    image = self.docker_client.images.get(f'odoo-openupgrade:{target}')
                    console.print("[yellow]Image found, attempting to continue...[/yellow]")
                except docker.errors.ImageNotFound:
                    console.print("[red]Image was not created. Build failed completely.[/red]")
                    raise e

            # Create output directory
            self.output_dir.mkdir(exist_ok=True, parents=True)

            # Run upgrade container with all necessary parameters
            console.print("Starting upgrade process...")

            upgrade_container = self.docker_client.containers.run(
                image=image.id,
                name='odoo-openupgrade',
                environment={
                    'HOST': 'db-odooupgrade',
                    'POSTGRES_USER': 'odoo',
                    'POSTGRES_PASSWORD': 'odoo',
                    'DB_HOST': 'db-odooupgrade',
                    'DB_PORT': '5432'
                },
                command=[
                    'bash', '-c',
                    'if [ -d /mnt/extra-addons/openupgrade_scripts/scripts ]; then '
                    'odoo -d database '
                    '--db_host=db-odooupgrade --db_port=5432 --db_user=odoo --db_password=odoo '
                    '--upgrade-path=/mnt/extra-addons/openupgrade_scripts/scripts '
                    '--update all --stop-after-init '
                    '--load=base,web,openupgrade_framework --log-level=info; '
                    'else '
                    'echo "Directory /mnt/extra-addons/openupgrade_scripts/scripts not found"; '
                    'ls -la /mnt/extra-addons/; '
                    'odoo -d database --db_host=db-odooupgrade --db_port=5432 --db_user=odoo --db_password=odoo '
                    '--update all --stop-after-init --log-level=info; '
                    'fi'
                ],
                network='odooupgrade-connection',
                detach=True,
                remove=False
            )

            # Monitor upgrade progress
            spinner_chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
            spinner_index = 0

            while upgrade_container.status in ['created', 'running']:
                console.print(
                    f"\r{spinner_chars[spinner_index % len(spinner_chars)]} "
                    f"Upgrade in progress... {time.strftime('%H:%M:%S')}",
                    end=""
                )
                spinner_index += 1
                time.sleep(1)
                upgrade_container.reload()

            console.print("\r" + " " * 60, end="\r")  # Clear line

            # Check exit code
            result = upgrade_container.wait()
            exit_code = result['StatusCode']

            # Get logs
            logs = upgrade_container.logs().decode()

            # Cleanup upgrade container and image
            try:
                upgrade_container.remove(force=True)
            except:
                pass

            try:
                self.docker_client.images.remove(image.id, force=True)
            except:
                pass

            try:
                dockerfile_path.unlink(missing_ok=True)
            except:
                pass

            if exit_code == 0:
                console.print(f"[green]✓[/green] Successfully upgraded to {target}")
                return True
            else:
                console.print(f"[yellow]⚠[/yellow] Upgrade exited with code {exit_code}")

                # Check if version was actually updated despite the error
                try:
                    new_version = self.get_current_version(container)
                    if new_version != current:
                        console.print(f"[yellow]Version was updated to {new_version} despite errors[/yellow]")
                        console.print(f"[yellow]This may be normal for OpenUpgrade - continuing...[/yellow]")
                        return True
                except:
                    pass

                if self.verbose:
                    console.print("\n[yellow]Last 30 log lines:[/yellow]")
                    for line in logs.split('\n')[-30:]:
                        if line.strip():
                            console.print(f"  {line}")
                else:
                    console.print("\n[yellow]Last 10 log lines:[/yellow]")
                    for line in logs.split('\n')[-10:]:
                        if line.strip():
                            console.print(f"  {line}")

                # Ask if user wants to continue
                console.print("\n[yellow]The upgrade process had errors, but the database might be partially upgraded.[/yellow]")
                return False

        except Exception as e:
            self.logger.error(f"Upgrade error: {e}")
            console.print(f"[red]✗[/red] Upgrade failed: {e}")
            return False

    def create_final_package(self, container: docker.models.containers.Container) -> bool:
        """Create final upgraded database package"""
        try:
            console.print("Creating final backup...")

            # Dump database
            exit_code, output = container.exec_run(
                'pg_dump -U odoo database'
            )

            if exit_code != 0:
                self.logger.error(f"Failed to dump database: {output.decode()}")
                return False

            # Save dump to output directory
            self.output_dir.mkdir(exist_ok=True, parents=True)
            dump_path = self.output_dir / 'dump.sql'
            dump_path.write_bytes(output)

            # Create ZIP package
            import zipfile
            zip_path = self.output_dir / 'upgraded.zip'

            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(dump_path, 'dump.sql')

                # Add filestore if exists
                filestore_dir = self.work_dir / 'filestore'
                if filestore_dir.exists():
                    console.print("Adding filestore to package...")
                    for file in filestore_dir.rglob('*'):
                        if file.is_file():
                            arcname = file.relative_to(self.work_dir)
                            zipf.write(file, arcname)

            # Cleanup temp dump file
            dump_path.unlink(missing_ok=True)

            console.print(f"[green]✓[/green] Created {zip_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create package: {e}")
            return False

    def run(self) -> bool:
        """Main upgrade workflow"""
        console.print("[bold blue]Odoo Database Upgrade Tool[/bold blue]")
        console.print(f"Source: {self.source}")
        console.print(f"Target: {self.target_version}\n")

        # Create directories early
        self.work_dir.mkdir(exist_ok=True, parents=True)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        db_container = None

        try:
            # Check prerequisites
            if not self.check_prerequisites():
                return False

            # Download or load source
            source_file = self.download_or_load_source()

            # Extract source
            dump_file, file_type = self.extract_source(source_file)

            # Create database container
            db_container = self.create_database_container()

            # Restore database
            if not self.restore_database(db_container, dump_file, file_type):
                return False

            # Get current version
            current_version = self.get_current_version(db_container)
            console.print(f"Current version: [cyan]{current_version}[/cyan]")
            console.print(f"Target version: [cyan]{self.target_version}[/cyan]\n")

            # Upgrade loop
            while self._compare_versions(current_version, self.target_version) < 0:
                next_version = self._get_next_version(current_version)

                if not self.upgrade_to_version(db_container, current_version, next_version):
                    return False

                # Get updated version
                current_version = self.get_current_version(db_container)
                console.print(f"Current version after upgrade: [cyan]{current_version}[/cyan]\n")

            # Create final package
            if not self.create_final_package(db_container):
                return False

            console.print("\n[bold green]✓ Upgrade completed successfully![/bold green]")
            console.print(f"Output available at: [cyan]{self.output_dir / 'upgraded.zip'}[/cyan]")
            return True

        except Exception as e:
            self.logger.error(f"Upgrade failed: {e}")
            console.print(f"\n[red]✗[/red] Upgrade failed: {e}")
            return False

        finally:
            # Cleanup
            self.cleanup(db_container)

    def cleanup(self, container: Optional[docker.models.containers.Container] = None):
        """Cleanup resources"""
        try:
            if container:
                container.stop(timeout=10)
                container.remove(force=True)

            # Remove network
            try:
                network = self.docker_client.networks.get('odooupgrade-connection')
                network.remove()
            except docker.errors.NotFound:
                pass
            except Exception as e:
                self.logger.debug(f"Network cleanup error: {e}")

            # Remove work directory
            if self.work_dir.exists():
                shutil.rmtree(self.work_dir, ignore_errors=True)

            console.print("[dim]Cleanup completed[/dim]")
        except Exception as e:
            self.logger.debug(f"Cleanup error: {e}")

    def _compare_versions(self, v1: str, v2: str) -> int:
        """Compare version strings
        Returns: -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
        """
        def version_tuple(v):
            return tuple(map(int, v.split('.')))

        v1_tuple = version_tuple(v1)
        v2_tuple = version_tuple(v2)

        if v1_tuple < v2_tuple:
            return -1
        elif v1_tuple == v2_tuple:
            return 0
        else:
            return 1

    def _get_next_version(self, current: str) -> str:
        """Get next upgrade version"""
        major = int(current.split('.')[0])
        return f"{major + 1}.0"


@click.command()
@click.option('--source', required=True, help='Path to database file or URL')
@click.option('--version', 'target_version', required=True, help='Target Odoo version')
@click.option('--verbose', is_flag=True, help='Enable verbose output')
@click.option('--log-file', help='Write logs to file')
def main(source: str, target_version: str, verbose: bool, log_file: Optional[str]):
    """Odoo Database Upgrade Tool

    Upgrade Odoo databases from version 10.0 to 18.0 using OpenUpgrade.

    Example:
        python test.py --source backup.zip --version 16.0
    """
    # Validate version
    valid_versions = ["10.0", "11.0", "12.0", "13.0", "14.0", "15.0", "16.0", "17.0", "18.0"]
    if target_version not in valid_versions:
        console.print(f"[red]Invalid version: {target_version}[/red]")
        console.print(f"Valid versions: {', '.join(valid_versions)}")
        raise click.Abort()

    upgrader = OdooUpgrader(source, target_version, verbose)
    success = upgrader.run()

    if not success:
        console.print("\n[red]Upgrade failed![/red]")
        raise click.Abort()


if __name__ == '__main__':
    main()