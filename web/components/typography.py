from nicegui import ui
import html

class SemanticHeading(ui.html):
    """
    A semantic heading element (h1-h6) that behaves like a UI element.
    It wraps the heading in a ui.html component but uses 'display: contents'
    to avoid layout issues with the wrapper div.
    It exposes a .text property to update content dynamically.
    """
    def __init__(self, tag: str, text: str, classes: str = '', style: str = ''):
        self.tag_name = tag
        self.heading_classes = classes
        self.heading_style = style
        self._text = text

        # Build initial HTML
        content = self._build_html(text)
        # We construct safe HTML manually, so we disable auto-sanitization to preserve our tags
        super().__init__(content, sanitize=False)

        # Make the wrapper transparent to layout
        self.style('display: contents')

    def _build_html(self, text):
        return f'<{self.tag_name} class="{self.heading_classes}" style="{self.heading_style}">{html.escape(text)}</{self.tag_name}>'

    @property
    def text(self):
        return self._text

    @text.setter
    def text(self, value):
        self._text = value
        self.content = self._build_html(value)

def h1(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 1 semantic heading."""
    return SemanticHeading('h1', text, classes, style)

def h2(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 2 semantic heading."""
    return SemanticHeading('h2', text, classes, style)

def h3(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 3 semantic heading."""
    return SemanticHeading('h3', text, classes, style)

def h4(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 4 semantic heading."""
    return SemanticHeading('h4', text, classes, style)

def h5(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 5 semantic heading."""
    return SemanticHeading('h5', text, classes, style)

def h6(text: str, classes: str = '', style: str = '') -> SemanticHeading:
    """Create a level 6 semantic heading."""
    return SemanticHeading('h6', text, classes, style)
