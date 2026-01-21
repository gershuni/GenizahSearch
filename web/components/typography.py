from nicegui import ui

def h1(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 1 semantic heading."""
    return ui.element('h1').text(text).classes(classes).style(style)

def h2(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 2 semantic heading."""
    return ui.element('h2').text(text).classes(classes).style(style)

def h3(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 3 semantic heading."""
    return ui.element('h3').text(text).classes(classes).style(style)

def h4(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 4 semantic heading."""
    return ui.element('h4').text(text).classes(classes).style(style)

def h5(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 5 semantic heading."""
    return ui.element('h5').text(text).classes(classes).style(style)

def h6(text: str, classes: str = '', style: str = '') -> ui.element:
    """Create a level 6 semantic heading."""
    return ui.element('h6').text(text).classes(classes).style(style)
