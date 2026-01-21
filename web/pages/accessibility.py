# -*- coding: utf-8 -*-
"""
Accessibility Statement Page

Required for Israeli Standard 5568 compliance.
"""

from nicegui import ui
from web.translations import tr, is_rtl
from web.components.typography import h1, h2, h3

def create_accessibility_page():
    """Create the Accessibility Statement page."""

    with ui.column().classes('w-full max-w-4xl mx-auto gap-8 fade-in p-6'):

        # Header
        h1(tr('Accessibility Statement'), classes='text-3xl font-bold', style='color: var(--primary-800);')

        # Introduction
        with ui.card().classes('w-full p-6'):
            ui.markdown('''
            **Genizah Search Pro** is committed to ensuring digital accessibility for people with disabilities.
            We are continually improving the user experience for everyone, and applying the relevant accessibility standards.
            ''').style('font-size: 1.1em;')

        # Conformance Status
        with ui.card().classes('w-full p-6'):
            h2(tr('Conformance Status'), classes='text-xl font-bold mb-4', style='color: var(--primary-700);')
            ui.markdown('''
            The Web Content Accessibility Guidelines (WCAG) defines requirements for designers and developers to improve accessibility for people with disabilities. It defines three levels of conformance: Level A, Level AA, and Level AAA.

            **Genizah Search Pro** is partially conformant with **WCAG 2.0 Level AA** and **Israeli Standard 5568**.

            "Partially conformant" means that some parts of the content do not fully conform to the accessibility standard, primarily due to the nature of historical manuscript images.
            ''')

        # Measures Taken
        with ui.card().classes('w-full p-6'):
            h2(tr('Measures to Support Accessibility'), classes='text-xl font-bold mb-4', style='color: var(--primary-700);')
            ui.markdown('''
            We have taken the following measures to ensure accessibility:

            *   **Keyboard Navigation**: The site is fully navigable using a keyboard.
            *   **Focus Visibility**: Focus indicators are clearly visible on all interactive elements.
            *   **Text Alternatives**: Controls and images have appropriate alternative text or labels.
            *   **Contrast**: Colors have been chosen to meet contrast requirements.
            *   **Zoom Support**: The site supports standard browser zoom up to 200% without loss of functionality.
            *   **Semantic Structure**: We use semantic HTML headings and landmarks to aid screen reader navigation.
            ''')

        # Limitations
        with ui.card().classes('w-full p-6'):
            h2(tr('Known Limitations'), classes='text-xl font-bold mb-4', style='color: var(--primary-700);')
            ui.markdown('''
            Despite our best efforts, there may be some limitations:

            1.  **Manuscript Images**: Scanned historical manuscripts are images of text and cannot be read directly by screen readers. We provide machine-generated transcriptions where available to assist with access.
            2.  **Generated Transcriptions**: The OCR (Optical Character Recognition) text may contain errors and might not perfectly reflect the manuscript content.
            ''')

        # Feedback & Contact
        with ui.card().classes('w-full p-6 border-l-4').style('border-left-color: var(--primary-600);'):
            h2(tr('Feedback and Contact'), classes='text-xl font-bold mb-4', style='color: var(--primary-700);')
            ui.markdown('''
            We welcome your feedback on the accessibility of Genizah Search Pro. Please let us know if you encounter accessibility barriers:

            *   **Email**: `gershuni [at] gmail [dot] com`

            We try to respond to feedback within 5 business days.
            ''')

        # Footer Date
        ui.label(f"{tr('Last Updated')}: February 2025").classes('text-sm text-gray-500 mt-8')
