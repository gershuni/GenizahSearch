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
    Merge and remove duplicate lists for a user.

    Args:
        user_id: The user's UUID
        dry_run: If True, only show what would be done without actually doing it
    """
    print(f"\n{'='*60}")
    print(f"Cleaning up duplicate lists for user: {user_id}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will merge & delete)'}")
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
    merge_operations = []  # (keep_id, dup_id, items_to_move)
    lists_to_delete = []

    for name, lists in by_name.items():
        if len(lists) > 1:
            # Sort by item count descending, then by ID ascending (prefer list with more items, then oldest)
            for lst in lists:
                items_response = client.table('list_items').select('id').eq('list_id', lst['id']).execute()
                lst['_item_count'] = len(items_response.data or [])

            lists.sort(key=lambda x: (-x['_item_count'], x['id']))
            keep = lists[0]
            duplicates = lists[1:]

            print(f"List '{name}':")
            print(f"  KEEP: ID {keep['id']} ({keep['_item_count']} items)")

            # Get existing sys_ids in keeper to avoid duplicates
            keep_items = client.table('list_items').select('sys_id').eq('list_id', keep['id']).execute()
            keep_sys_ids = {item['sys_id'] for item in (keep_items.data or [])}

            for dup in duplicates:
                # Get items from duplicate
                dup_items = client.table('list_items').select('*').eq('list_id', dup['id']).execute()
                dup_item_list = dup_items.data or []

                # Find items to move (not already in keeper)
                items_to_move = [item for item in dup_item_list if item['sys_id'] not in keep_sys_ids]

                print(f"  MERGE & DELETE: ID {dup['id']} ({dup['_item_count']} items, {len(items_to_move)} unique to move)")

                merge_operations.append((keep['id'], dup['id'], items_to_move))
                lists_to_delete.append(dup['id'])

                # Add moved items to keeper set to avoid duplicates from other dups
                for item in items_to_move:
                    keep_sys_ids.add(item['sys_id'])
            print()

    if not lists_to_delete:
        print("No duplicates found!")
        return

    total_items_to_move = sum(len(items) for _, _, items in merge_operations)
    print(f"\nSummary:")
    print(f"  Lists to merge & delete: {len(lists_to_delete)}")
    print(f"  Items to move: {total_items_to_move}")

    if dry_run:
        print(f"\nThis was a DRY RUN. To actually merge & delete, run with --delete flag")
        return

    # Actually merge and delete
    print(f"\nMerging and deleting...")

    for keep_id, dup_id, items_to_move in merge_operations:
        # Move items to keeper list
        for item in items_to_move:
            # Insert into keeper list
            new_item = {
                'list_id': keep_id,
                'sys_id': item['sys_id'],
                'user_id': item['user_id'],
                'added_at': item.get('added_at'),
                'notes': item.get('notes'),
            }
            try:
                client.table('list_items').insert(new_item).execute()
            except Exception as e:
                print(f"    Warning: Could not move item {item['sys_id']}: {e}")

        # Delete items from duplicate
        client.table('list_items').delete().eq('list_id', dup_id).execute()
        # Delete the duplicate list
        client.table('user_lists').delete().eq('id', dup_id).execute()
        print(f"  Merged {len(items_to_move)} items and deleted list ID {dup_id}")

    print(f"\nDone! Merged items and deleted {len(lists_to_delete)} duplicate lists.")


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
