# -*- coding: utf-8 -*-
"""
Interactive Config Generator - Asks user about each symbol type and saves decisions.

This script reads the symbol report and interactively asks the user
what to do with each type of special symbol/pattern found.
"""

import os
import json
from typing import Dict, Any

from .config import SYMBOL_REPORT_FILE, CLEANING_RULES_FILE, ensure_dirs


# Available actions for text patterns
ACTIONS = {
    'keep': {
        'name': 'Keep as-is',
        'name_he': 'שמור כמו שהוא',
        'description': 'Keep the entire pattern including markers'
    },
    'remove_entirely': {
        'name': 'Remove entirely',
        'name_he': 'הסר לחלוטין',
        'description': 'Remove the entire pattern including content'
    },
    'remove_markers_keep_content': {
        'name': 'Remove markers, keep content',
        'name_he': 'הסר סימנים, שמור תוכן',
        'description': 'Remove the surrounding markers but keep the text inside'
    },
    'extract_as_metadata': {
        'name': 'Extract as metadata',
        'name_he': 'חלץ כמטא-דאטא',
        'description': 'Remove from text but save as metadata'
    },
    'replace_with_space': {
        'name': 'Replace with space',
        'name_he': 'החלף ברווח',
        'description': 'Replace the pattern with a single space'
    },
    'prefer_content': {
        'name': 'Prefer bracketed content (correction)',
        'name_he': 'העדף תוכן בסוגריים (תיקון)',
        'description': 'Use the bracketed text as the preferred reading'
    },
    'prefer_outside': {
        'name': 'Prefer outside content (original)',
        'name_he': 'העדף טקסט מחוץ לסוגריים (מקור)',
        'description': 'Keep the text outside brackets, remove bracketed content'
    },
    'custom': {
        'name': 'Custom regex replacement',
        'name_he': 'החלפה מותאמת אישית',
        'description': 'Define a custom regex replacement rule'
    }
}


class InteractiveConfig:
    """Interactive configuration generator for cleaning rules."""

    def __init__(self, symbol_report_path: str = None):
        self.symbol_report_path = symbol_report_path or SYMBOL_REPORT_FILE
        self.rules = {
            'version': '1.0',
            'corpora': {}
        }
        self._load_symbol_report()

    def _load_symbol_report(self):
        """Load the symbol discovery report."""
        if not os.path.exists(self.symbol_report_path):
            raise FileNotFoundError(
                f"Symbol report not found: {self.symbol_report_path}\n"
                "Run symbol_discovery.py first."
            )

        with open(self.symbol_report_path, 'r', encoding='utf-8') as f:
            self.symbol_report = json.load(f)

    def _print_header(self, text: str, char: str = '='):
        """Print a formatted header."""
        print(f"\n{char * 60}")
        print(f"  {text}")
        print(char * 60)

    def _print_examples(self, examples: list, max_show: int = 5):
        """Print example occurrences."""
        print("\n  דוגמאות / Examples:")
        for i, ex in enumerate(examples[:max_show]):
            # Truncate long examples
            display = ex[:80] + '...' if len(ex) > 80 else ex
            print(f"    {i+1}. {display}")

    def _get_action_choice(self, pattern_name: str, suggested: str) -> Dict[str, Any]:
        """Ask user to choose an action for a pattern."""
        print("\n  אפשרויות / Options:")
        action_list = list(ACTIONS.items())

        # Put suggested action first
        if suggested in ACTIONS:
            action_list = [(suggested, ACTIONS[suggested])] + \
                         [(k, v) for k, v in action_list if k != suggested]

        for i, (action_id, action_info) in enumerate(action_list):
            marker = " [מומלץ/recommended]" if action_id == suggested else ""
            print(f"    {i+1}. {action_info['name_he']} / {action_info['name']}{marker}")

        print(f"    0. דלג / Skip")
        print(f"    q. סיים / Quit configuration")

        while True:
            choice = input("\n  בחירה / Choice [1]: ").strip()

            if choice.lower() == 'q':
                return {'action': 'quit'}

            if choice == '' or choice == '1':
                # Default to first option (suggested)
                return {'action': action_list[0][0]}

            if choice == '0':
                return {'action': 'skip'}

            try:
                idx = int(choice) - 1
                if 0 <= idx < len(action_list):
                    action_id = action_list[idx][0]

                    # If custom, ask for regex
                    if action_id == 'custom':
                        print("\n  הגדר ביטוי רגולרי להחלפה / Define regex replacement:")
                        pattern = input("    Pattern: ").strip()
                        replacement = input("    Replacement: ").strip()
                        return {
                            'action': 'custom',
                            'pattern': pattern,
                            'replacement': replacement
                        }

                    return {'action': action_id}

            except ValueError:
                pass

            print("  בחירה לא תקינה / Invalid choice")

    def configure_corpus(self, corpus_id: str) -> Dict[str, Any]:
        """Interactively configure rules for a corpus."""
        corpus_data = self.symbol_report.get('corpora', {}).get(corpus_id)
        if not corpus_data:
            print(f"No data for corpus: {corpus_id}")
            return {}

        self._print_header(f"{corpus_data.get('name_he', corpus_id)} / {corpus_data.get('name', corpus_id)}")

        rules = {'patterns': {}}
        patterns = corpus_data.get('patterns', {})

        # Sort by count descending
        sorted_patterns = sorted(patterns.items(), key=lambda x: x[1]['count'], reverse=True)

        for pattern_name, pattern_info in sorted_patterns:
            count = pattern_info['count']
            examples = pattern_info.get('examples', [])
            suggested = pattern_info.get('suggested_action', 'ask_user')

            self._print_header(f"Pattern: {pattern_name} ({count} occurrences)", '-')
            self._print_examples(examples)

            result = self._get_action_choice(pattern_name, suggested)

            if result['action'] == 'quit':
                print("\nשומר הגדרות חלקיות... / Saving partial configuration...")
                break
            elif result['action'] != 'skip':
                rules['patterns'][pattern_name] = result

        return rules

    def run_interactive(self):
        """Run the full interactive configuration process."""
        self._print_header("הגדרת כללי ניקוי טקסט / Text Cleaning Rules Configuration")

        print("""
  סקריפט זה ישאל אותך מה לעשות עם כל סוג של סימון מיוחד שנמצא בקורפוס.
  This script will ask you what to do with each type of special symbol found.

  הכללים יישמרו וישמשו לניקוי הטקסט לפני החיפוש.
  The rules will be saved and used to clean text before searching.
        """)

        input("לחץ Enter להמשך / Press Enter to continue...")

        for corpus_id, corpus_data in self.symbol_report.get('corpora', {}).items():
            corpus_rules = self.configure_corpus(corpus_id)
            self.rules['corpora'][corpus_id] = corpus_rules

            if corpus_rules.get('patterns', {}).get('quit'):
                break

            # Ask to continue to next corpus
            if corpus_id != list(self.symbol_report['corpora'].keys())[-1]:
                cont = input("\nלהמשיך לקורפוס הבא? / Continue to next corpus? [Y/n]: ").strip().lower()
                if cont == 'n':
                    break

        self.save_rules()
        return self.rules

    def save_rules(self, filepath: str = None):
        """Save the configuration rules to a JSON file."""
        if filepath is None:
            ensure_dirs()
            filepath = CLEANING_RULES_FILE

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.rules, f, ensure_ascii=False, indent=2)

        print(f"\n{'='*60}")
        print(f"  כללים נשמרו ב / Rules saved to:")
        print(f"  {filepath}")
        print('='*60)

        return filepath

    def load_existing_rules(self, filepath: str = None) -> Dict[str, Any]:
        """Load existing rules from a file."""
        if filepath is None:
            filepath = CLEANING_RULES_FILE

        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                self.rules = json.load(f)
            print(f"Loaded existing rules from: {filepath}")
            return self.rules

        return {}


def run_configuration(symbol_report_path: str = None):
    """
    Run the interactive configuration process.

    Args:
        symbol_report_path: Optional path to symbol report

    Returns:
        Path to saved rules file
    """
    config = InteractiveConfig(symbol_report_path)
    config.run_interactive()
    return CLEANING_RULES_FILE


if __name__ == '__main__':
    run_configuration()
