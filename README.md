# OdooUpgrade

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)

Professional command-line tool for automating Odoo database upgrades using [OCA's OpenUpgrade](https://github.com/OCA/OpenUpgrade) framework. Seamlessly upgrade your Odoo databases from version 10.0 through 18.0 with a single command.

## ✨ Features

- **🚀 Automated Incremental Upgrades**: Automatically handles multi-step upgrades
- **📦 Multiple Source Formats**: Supports both `.zip` and `.dump` database files
- **🌐 Remote Downloads**: Download databases directly from URLs
- **🐳 Docker-Based**: Uses containerized environments for safe, isolated upgrades
- **📊 Rich CLI Output**: Beautiful progress bars and status indicators using Rich library
- **🔄 Resume Support**: Intelligent version detection to continue from current state
- **📝 Detailed Logging**: Optional verbose mode and log file support
- **✅ Validation**: Pre-flight checks for source accessibility and Docker availability

## 📋 Requirements

- **Python**: 3.9 or higher
- **Docker**: Docker Engine with Docker Compose (v2) or docker-compose (v1)
- **Operating System**: Linux, macOS, or Windows (with WSL2 for best results)
- **Disk Space**: Minimum 5GB free space for Docker volumes and temporary files

## 🚀 Installation

### Using pip (Recommended)

```bash
pip install odooupgrader
```

### From Source

```bash
git clone https://github.com/fasilwdr/OdooUpgrader.git
cd OdooUpgrader
pip install -e .
```

## 📖 Usage

### Basic Usage

Upgrade a local database file to version 16.0:

```bash
odooupgrader --source /path/to/database.zip --version 16.0
```

### Download and Upgrade from URL

```bash
odooupgrader --source https://example.com/database.dump --version 17.0
```

### Specify PostgreSQL Version

```bash
odooupgrader --source /path/to/database.zip --version 16.0 --postgres-version 15
```

### Enable Verbose Logging

```bash
odooupgrader --source /path/to/database.zip --version 18.0 --verbose
```

### Save Logs to File

```bash
odooupgrader --source /path/to/database.zip --version 15.0 --log-file upgrade.log
```

## 🎯 Command-Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--source` | ✅ | Path to local `.zip`/`.dump` file or URL to download |
| `--version` | ✅ | Target Odoo version (10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0) |
| `--postgres-version` | ❌ | PostgreSQL version for the database container (default: 13) |
| `--verbose` | ❌ | Enable verbose logging output |
| `--log-file` | ❌ | Path to save detailed log file |

## 🔄 How It Works

1. **Validation**: Checks if source file/URL is accessible and Docker is available
2. **Environment Setup**: Creates necessary directories and PostgreSQL container
3. **Source Processing**: Downloads (if URL) and extracts the database
4. **Database Restoration**: Restores database and filestore to PostgreSQL
5. **Version Detection**: Determines current database version
6. **Incremental Upgrades**: Runs OpenUpgrade for each version step
7. **Package Creation**: Creates final `.zip` with upgraded database and filestore

## 📁 Output Structure

After successful upgrade, you'll find in the `output` directory:

```
output/
├── upgraded.zip          # Final packaged database (ready to restore)
└── odoo.log             # Upgrade process logs
```


## 🔍 Supported Versions

This tool supports upgrading Odoo databases from version 10.0 through 18.0. The upgrade paths and compatibility are determined by the [OCA OpenUpgrade project](https://github.com/OCA/OpenUpgrade), which maintains migration scripts for each Odoo version.

## 🤝 Contributing

Contributions are welcome!

## 🔐 Security Considerations

- Database credentials are hardcoded for the temporary Docker container
- The PostgreSQL container is on an isolated Docker network
- No ports are exposed to the host machine
- Containers are automatically cleaned up after upgrade
- Consider using this tool in isolated environments for production databases

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [OCA (Odoo Community Association)](https://odoo-community.org/) for the OpenUpgrade framework
- [OpenUpgrade Project](https://github.com/OCA/OpenUpgrade) for migration scripts

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/fasilwdr/OdooUpgrade/issues)
- **Email**: fasilwdr@hotmail.com

## 📈 Changelog

### Version 0.1.0 (Initial Release)

- ✨ Initial release with core upgrade functionality
- ✅ Support for Odoo versions 10.0 through 18.0
- 🐳 Docker-based isolated upgrade environment
- 📊 Rich CLI output with progress indicators
- 🌐 URL download support for remote databases
- 📦 Automatic packaging of upgraded databases

---

**Made with ❤️ by Fasil | Powered by OpenUpgrade**

⭐ If you find this tool helpful, please star the repository!