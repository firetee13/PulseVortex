# Contributing to PulseVortex Monitor

Thank you for your interest in contributing to PulseVortex Monitor! This guide will help you get started with contributing to this trading analysis project.

## 🚀 Quick Start

### Prerequisites

- Python 3.8+ (we test against 3.8-3.12)
- MetaTrader 5 (for development and testing)
- Git
- Windows, macOS, or Linux (GUI requires Windows for full MT5 functionality)

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/yourusername/monitor_prod.git
   cd monitor_prod
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install in Development Mode**
   ```bash
   pip install -e .[dev]
   ```

4. **Verify Installation**
   ```bash
   pytest --version
   monitor-setup --help
   monitor-gui --help
   ```

## 🏗️ Project Structure

```
monitor_prod/
├── src/monitor/          # Main package
│   ├── cli/             # Command-line interfaces
│   ├── gui/             # GUI application
│   ├── core/            # Core business logic
│   └── scripts/         # Utility scripts
├── tests/               # Test suite
├── docs/                # Documentation
├── .github/             # GitHub workflows
├── pyproject.toml       # Package configuration
└── README.md            # Project documentation
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=monitor --cov-report=html

# Run specific test file
pytest tests/test_config.py

# Run with verbose output
pytest -v
```

### Test Structure

- **Unit Tests**: Test individual functions and classes
- **Integration Tests**: Test component interactions
- **Mock MT5**: All tests use mocked MT5 connections

### Writing Tests

1. **Follow Naming Convention**: `test_*.py` files, `test_*()` functions
2. **Use Fixtures**: Leverage pytest fixtures for setup
3. **Mock External Dependencies**: Use unittest.mock for MT5, database
4. **Cover Edge Cases**: Test error conditions and boundary cases

```python
import pytest
from unittest.mock import patch
from monitor.core.config import default_db_path

def test_default_db_path():
    """Test that default database path is correctly resolved."""
    path = default_db_path()
    assert path.name == "timelapse.db"
    assert path.parent.exists()

@patch('monitor.core.mt5_client.MT5')
def test_mt5_connection(mock_mt5):
    """Test MT5 connection handling."""
    # Your test code here
    pass
```

## 📝 Code Style

We use automated tools to maintain code quality:

### Formatting
- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting

```bash
# Format code
black src tests
isort src tests

# Check linting
flake8 src tests
```

### Type Checking
We use **mypy** for static type checking:

```bash
mypy src/monitor
```

### Pre-commit Hooks
Set up pre-commit hooks to automatically check code quality:

```bash
pip install pre-commit
pre-commit install
```

## 🔧 Development Workflow

### 1. Create Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-fix-name
```

### 2. Make Changes
- Write clean, well-documented code
- Add tests for new functionality
- Ensure all tests pass
- Follow commit message conventions

### 3. Commit
```bash
# Stage changes
git add .

# Commit with conventional message
git commit -m "feat(gui): add new chart type for PnL analysis"
```

### 4. Test
```bash
# Run full test suite
pytest

# Check code quality
black --check src tests
isort --check-only src tests
flake8 src tests
mypy src/monitor
```

### 5. Push and Create PR
```bash
git push origin feature/your-feature-name
```

## 📋 Commit Message Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements
- `ci`: CI/CD changes

### Examples
```
feat(gui): add real-time chart updates
fix(mt5): handle connection timeout gracefully
docs(readme): update installation instructions
test(core): add tests for database migration
```

## 🏷️ Labels and Issues

### Issue Types
- **bug**: Unexpected behavior or crashes
- **enhancement**: New features or improvements
- **documentation**: Documentation issues
- **question**: General questions
- **performance**: Performance-related issues

### Labels
- `gui`: GUI-related changes
- `cli`: Command-line interface
- `database`: Database-related
- `mt5-integration`: MetaTrader 5 integration
- `good first issue`: Good for newcomers
- `help wanted`: Community help requested

## 🐛 Bug Reports

When reporting bugs, please include:

1. **Environment Information**
   - OS and Python version
   - MetaTrader 5 version
   - Package version

2. **Steps to Reproduce**
   - Clear, step-by-step instructions
   - Minimal reproducible example

3. **Expected vs Actual Behavior**
   - What you expected to happen
   - What actually happened

4. **Logs and Error Messages**
   - Full error traceback
   - Relevant log files

5. **Additional Context**
   - Screenshots if applicable
   - Configuration files (sanitized)

## 💡 Feature Requests

When suggesting features:

1. **Use a clear title**
2. **Describe the problem** you're trying to solve
3. **Propose a solution** if you have one
4. **Consider alternatives** and trade-offs
5. **Provide examples** or mockups if applicable

## 🔒 Security

### Security Considerations
- Never commit API keys, passwords, or credentials
- Use environment variables for sensitive configuration
- Follow secure coding practices
- Report security vulnerabilities privately

### Reporting Security Issues
For security vulnerabilities, please email: security@example.com

## 📊 Performance

### Performance Guidelines
- Database queries should be optimized
- Avoid blocking operations in GUI
- Use caching for expensive computations
- Profile code before and after optimizations

### Performance Testing
```bash
# Install profiling tools
pip install pytest-benchmark memory_profiler

# Run benchmarks
pytest --benchmark-only
```

## 📚 Documentation

### Documentation Types
- **API Documentation**: Docstrings for public APIs
- **User Documentation**: README, guides, tutorials
- **Developer Documentation**: Architecture, contributing guide
- **Code Comments**: Complex logic explanations

### Writing Documentation
- Use clear, concise language
- Include code examples
- Keep documentation up to date
- Use consistent formatting

## 🚀 Release Process

### Release Checklist
1. [ ] All tests pass
2. [ ] Documentation is updated
3. [ ] Version is updated
4. [ ] Changelog is updated
5. [ ] Tests run on all supported Python versions
6. [ ] Package builds successfully
7. [ ] Docker image builds and tests pass

### Release Types
- **Major**: Breaking changes (2.0.0)
- **Minor**: New features (1.1.0)
- **Patch**: Bug fixes (1.0.1)

## 🤝 Community

### Getting Help
- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For general questions
- **Documentation**: Check existing docs first

### Code of Conduct
We are committed to providing a welcoming and inclusive environment. Please:

- Be respectful and considerate
- Use inclusive language
- Focus on constructive feedback
- Help others learn and grow

## 📖 Additional Resources

### Python Resources
- [Python Packaging User Guide](https://packaging.python.org/)
- [pytest Documentation](https://docs.pytest.org/)
- [Black Code Formatter](https://black.readthedocs.io/)

### Trading/Finance Resources
- [MetaTrader 5 Python API](https://www.mql5.com/en/docs/integration/python_metatrader5)
- [Financial Data Analysis Best Practices](https://github.com/ranaroussi/yfinance)

### Development Tools
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docker Best Practices](https://pythonspeed.com/articles/docker-best-practices/)

---

## 🙏 Thank You!

Thank you for contributing to PulseVortex Monitor! Your contributions help make this project better for everyone.

If you have any questions or need help getting started, please don't hesitate to:

- Create an issue for questions
- Start a discussion
- Ask for help in your pull request

Happy coding! 🚀
