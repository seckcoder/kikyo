from kikyo import _state

app_or_default = None

def _app_or_default(app=None):
    if app is None:
        return _state.get_current_app()
    return app
app_or_default = _app_or_default
