
import sys
from PyQt6.QtWidgets import QApplication, QTreeWidget, QTreeWidgetItem, QLabel, QWidget, QVBoxLayout

def make_snippet_label(text_content):
    if not text_content: return QLabel("")
    lbl = QLabel(f"SNIPPET: {text_content}")
    return lbl

def main():
    app = QApplication(sys.argv)
    window = QWidget()
    layout = QVBoxLayout()
    tree = QTreeWidget()
    tree.setColumnCount(5)
    layout.addWidget(tree)
    window.setLayout(layout)
    window.show()

    # Mock Data
    # Item 1: Parent with children
    item1 = {
        'type': 'manuscript',
        'sys_id': '111',
        'pages': [
            {'text': 'Text A (Page 1)', 'score': 10, 'raw_header': 'H1'},
            {'text': 'Text B (Page 2)', 'score': 5, 'raw_header': 'H2'}
        ]
    }

    # Item 2: Parent with children (Should have Text C)
    item2 = {
        'type': 'manuscript',
        'sys_id': '222',
        'pages': [
            {'text': 'Text C (Page 1)', 'score': 10, 'raw_header': 'H3'},
            {'text': 'Text D (Page 2)', 'score': 5, 'raw_header': 'H4'}
        ]
    }

    items = [item1, item2]

    root = QTreeWidgetItem(tree, ["Root"])
    tree.addTopLevelItem(root)

    for i, ms_item in enumerate(items):
        print(f"Processing Item {i}: {ms_item['sys_id']}")

        ms_node = QTreeWidgetItem(root)
        ms_node.setText(0, "Score")
        ms_node.setText(1, f"Shelf {ms_item['sys_id']}")

        pages = ms_item.get('pages', [])

        if pages:
            p0 = pages[0]
            print(f"  Parent P0 Text: {p0['text']}")
            lbl_main = make_snippet_label(p0.get('text', ''))
            tree.setItemWidget(ms_node, 4, lbl_main)

            for p_item in pages:
                page_node = QTreeWidgetItem(ms_node)
                page_node.setText(1, "Page")
                print(f"    Child Text: {p_item['text']}")
                lbl = make_snippet_label(p_item.get('text', ''))
                tree.setItemWidget(page_node, 4, lbl)

    # Allow inspection
    print("Running...")
    # sys.exit(app.exec()) # Don't actually run loop in headless

if __name__ == "__main__":
    main()
