# -*- coding: utf-8 -*-
"""
Cleanup script to remove duplicate lists from Supabase.

This script:
1. Connects to Supabase using credentials from .env
2. Finds all lists for the current user
3. Groups lists by name
4. Keeps the oldest one (lowest ID) for each name
5. Deletes duplicates and their items

Run this script once to clean up duplicates created by sync issues.
"""

import os
from collections import defaultdict
from dotenv import load_dotenv

# Load environment
load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://ylcpglwxompwjcufdemz.supabase.co')
# Use service role key to bypass RLS for admin operations
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

if not SUPABASE_SERVICE_KEY:
    print("ERROR: SUPABASE_SERVICE_KEY not set in environment or .env file")
    print("This script requires the service role key to bypass Row Level Security.")
    exit(1)

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase package not installed. Run: pip install supabase")
    exit(1)


def cleanup_duplicates(user_id: str, dry_run: bool = True):
    """
    Remove duplicate lists for a user.

    Args:
        user_id: The user's UUID
        dry_run: If True, only show what would be deleted without actually deleting
    """
    print(f"\n{'='*60}")
    print(f"Cleaning up duplicate lists for user: {user_id}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will delete)'}")
    print(f"{'='*60}\n")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Get all lists for the user
    response = client.table('user_lists').select('*').eq('user_id', user_id).execute()
    all_lists = response.data or []

    print(f"Found {len(all_lists)} total lists\n")

    # Group by name
    by_name = defaultdict(list)
    for lst in all_lists:
        by_name[lst['name']].append(lst)

    # Find duplicates
    lists_to_delete = []
    items_to_delete_count = 0

    for name, lists in by_name.items():
        if len(lists) > 1:
            # Sort by ID (keep lowest = oldest)
            lists.sort(key=lambda x: x['id'])
            keep = lists[0]
            duplicates = lists[1:]

            print(f"List '{name}':")
            print(f"  KEEP: ID {keep['id']}")
            for dup in duplicates:
                # Count items in this list
                items_response = client.table('list_items').select('id').eq('list_id', dup['id']).execute()
                item_count = len(items_response.data or [])
                items_to_delete_count += item_count
                print(f"  DELETE: ID {dup['id']} ({item_count} items)")
                lists_to_delete.append(dup['id'])
            print()

    if not lists_to_delete:
        print("No duplicates found!")
        return

    print(f"\nSummary:")
    print(f"  Lists to delete: {len(lists_to_delete)}")
    print(f"  Items to delete: {items_to_delete_count}")

    if dry_run:
        print(f"\nThis was a DRY RUN. To actually delete, run with dry_run=False")
        return

    # Actually delete
    print(f"\nDeleting...")

    for list_id in lists_to_delete:
        # Delete items first (cascade should handle this, but be safe)
        client.table('list_items').delete().eq('list_id', list_id).execute()
        # Delete the list
        client.table('user_lists').delete().eq('id', list_id).execute()
        print(f"  Deleted list ID {list_id}")

    print(f"\nDone! Deleted {len(lists_to_delete)} duplicate lists.")


if __name__ == '__main__':
    import sys

    # Get user ID from command line or use the one from logs
    USER_ID = "b93c7c98-8bd7-4020-94e5-5a3450c357d3"  # From your logs

    # Check for --delete flag
    do_delete = '--delete' in sys.argv

    if len(sys.argv) > 1 and sys.argv[1] != '--delete':
        USER_ID = sys.argv[1]

    if do_delete:
        print("Running with --delete flag - will actually delete duplicates\n")
        cleanup_duplicates(USER_ID, dry_run=False)
    else:
        print("DRY RUN MODE - showing what would be deleted\n")
        print("To actually delete, run: python cleanup_duplicate_lists.py --delete\n")
        cleanup_duplicates(USER_ID, dry_run=True)
