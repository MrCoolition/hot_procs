from __future__ import annotations

from pathlib import Path

_APP_PATH = Path(__file__).with_name('streamlit_app.py')
_START_MARKER = '# === CORE_HELPERS_START ==='
_END_MARKER = '# === CORE_HELPERS_END ==='

_source = _APP_PATH.read_text()
start = _source.index(_START_MARKER) + len(_START_MARKER)
end = _source.index(_END_MARKER)
exec(compile(_source[start:end], str(_APP_PATH), 'exec'))
