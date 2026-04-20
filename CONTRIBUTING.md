# Contributing to FluxDB

Thank you for your interest in contributing to FluxDB!

## Development Setup

1. Clone the repository:
```bash
git clone https://github.com/moggan1337/FluxDB.git
cd FluxDB
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run tests:
```bash
pytest tests/ -v
```

## Code Style

We use:
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

Format your code:
```bash
black src/ tests/
isort src/ tests/
flake8 src/ tests/
mypy src/
```

## Project Structure

```
src/
├── core/           # Core storage and event log
├── event_sourcing/ # Event sourcing components
├── crdt/           # CRDT implementations
├── consensus/      # Raft consensus
├── replication/    # Distributed replication
└── api/            # Client API
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Commit your changes with clear messages
7. Push to your fork
8. Open a Pull Request

## Commit Messages

Use clear, descriptive commit messages:
- `Add LWW-Register CRDT implementation`
- `Fix time-travel query timestamp handling`
- `Add Raft leader election timeout`

## Testing

Write tests for all new functionality:
- Unit tests in `tests/`
- Integration tests for complex scenarios
- Examples in `examples/` for documentation

## Reporting Issues

Please report issues on GitHub with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Python version and environment

## Questions?

Feel free to open an issue for any questions!
