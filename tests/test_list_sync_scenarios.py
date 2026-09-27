# -*- coding: utf-8 -*-
"""The list sync's scenario gate: seeded random histories of two desktops and the website.

tests/list_sync_scenarios.py holds the fake Supabase, the actors, the operations
and the invariants; this file runs them. A failure prints the seed's shrunk op
list as a literal: paste it into REGRESSION_CASES below, with one line saying
what it shows, so it replays on every run.

Before a pull request that touches shared/lists_sync.py or the list bookkeeping of
shared/lists_manager.py, also run the long versions locally (see the harness's docstring):
    python tests/list_sync_scenarios.py --seeds 0-19999 --steps 60 --jobs 8
    python tests/list_sync_scenarios.py --seeds 0-1999 --steps 200 --jobs 8
    python tests/list_sync_scenarios.py --seeds 0-4999 --steps 60 --jobs 8 --mix removals
"""
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import list_sync_scenarios as S  # noqa: E402
from shared import lists_manager, lists_sync  # noqa: E402

CI_SEEDS = range(0, 500)
CI_STEPS = 60

# (what it shows, seed, config, ops) -- each must replay without a violation.
REGRESSION_CASES = [
    ('an entry the desktop matched by sys_id alone had its row moved into another list',
     2, {'has_page': True, 'max_rows': None, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
         'upgrade': 0},
     [('web', (9486, 54333, 50057, 35852, 34435)), ('sync', 'B', 'up', None),
      ('web', (49384, 56461, 63044, 48608, 26266))]),
    ('a second page of one manuscript took the first page\'s row, so two items held one cloud id',
     10, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'A', (22394, 53204, 16662, 1612, 40272)), ('sync', 'A', 'up', None),
      ('desk', 'A', (4658, 19529, 62103, 10191, 4605))]),
    ('a Download dropped a page from its list after the rows had been mixed up',
     10, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'A', (22394, 53204, 16662, 1612, 40272)), ('sync', 'A', 'up', None),
      ('desk', 'A', (4658, 19529, 62103, 10191, 4605)), ('sync', 'A', 'up', None),
      ('sync', 'A', 'merge', None)]),
    ('an upload wrote the empty local note over a note written on the website (tracker line 144)',
     6, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False,
         'upgrade': 0},
     [('sync', 'B', 'up', None), ('web', (33561, 56577, 39119, 58486, 57659)),
      ('desk', 'B', (11389, 55333, 63701, 41847, 63164))]),
    ('an upload put an older note over the website\'s edit and a Download then replaced the newer local '
     'note with it, losing it everywhere',
     55, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False,
          'upgrade': 11},
     [('desk', 'B', (30481, 4785, 25793, 25019, 34981)), ('sync', 'B', 'merge', None),
      ('web', (4034, 60271, 36893, 23592, 1753)), ('sync', 'A', 'merge', None)]),
    ('two folios of one manuscript on two computers overwrote each other\'s row and note',
     57, {'has_page': True, 'max_rows': 3, 'p_inject': 0.0, 'past_end_raises': False, 'page_lag': False,
          'upgrade': 0},
     [('desk', 'B', (44905, 19465, 34225, 53870, 53427)), ('desk', 'A', (31658, 9107, 55291, 22971, 52725))]),
    ('a My Library entry in a list was uploaded with its note and tags',
     116, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False,
           'upgrade': 0},
     [('desk', 'B', (59856, 49842, 25921, 7647, 62905))]),
    ('a My Library row already in the cloud was downloaded into a list',
     0, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': True,
         'upgrade': 0},
     []),
    # Found by this gate while the per-membership engine was built (each broke an invariant then):
    ('a live list that took a cloud list in the Trash got its entries only on the second Merge',
     9, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (37729, 49124, 62432, 28524, 27932)),
       ('sync', 'B', 'merge', (29, 'session_lost', (30258, 43090, 37654, 6059, 6828))),
       ('web', (18635, 17241, 32127, 63370, 41326)), ('sync', 'A', 'merge', None)]),
    ('without the page column, rows of a same-name list multiplied on every sync',
     15, {'has_page': False, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 14},
     [('web', (37710, 35104, 9227, 2991, 18220)), ('sync', 'B', 'down', None), ('sync', 'A', 'down', None),
       ('sync', 'B', 'up', (2, 'anon', (17927, 15835, 6535, 50001, 53713)))]),
    ('a same-name row brought back, one Merge later, a note the website had replaced',
     17, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (349, 54028, 22292, 11971, 45698)), ('web', (3021, 27231, 26150, 59730, 27553)),
       ('web', (21196, 41766, 55845, 3738, 27208)), ('sync', 'A', 'up', None),
       ('desk', 'A', (38184, 44863, 59618, 33490, 35253)), ('web', (43012, 2530, 19718, 23737, 29138)),
       ('sync', 'A', 'merge', (7, 'anon', (23952, 24353, 56852, 30968, 6035))),
       ('web', (8956, 17443, 18174, 25951, 54833)), ('desk', 'B', (34813, 8911, 35869, 16643, 12941)),
       ('sync', 'B', 'merge', None), ('web', (48269, 1099, 14191, 45707, 21089)),
       ('sync', 'B', 'merge', None)]),
    ('two computers that kept the same notes in another order nested them in each other forever',
     174, {'has_page': True, 'max_rows': 2, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (28999, 21509, 57447, 58682, 65269)), ('sync', 'B', 'up', None),
       ('web', (48219, 44215, 23688, 62533, 62094)), ('desk', 'B', (58603, 33571, 14417, 26957, 22538)),
       ('web', (38467, 59874, 4174, 16084, 32190)), ('sync', 'B', 'merge', None),
       ('sync', 'A', 'merge', None), ('desk', 'A', (59921, 59320, 47912, 17430, 47824)),
       ('web', (51142, 63111, 56881, 51619, 32762)), ('desk', 'B', (20369, 25133, 41341, 5807, 2508)),
       ('desk', 'A', (28772, 23903, 13293, 44543, 33280)), ('web', (18855, 36193, 59974, 23179, 43031))]),
    ('a tag dropped on one of two rows flipped between two computers forever',
     428, {'has_page': True, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': True, 'upgrade': 0},
     [('desk', 'B', (40394, 20529, 18465, 35068, 39156)), ('web', (62568, 55617, 50874, 17281, 59630)),
       ('sync', 'B', 'merge', (17, 'raise_before', (3121, 37997, 24376, 11240, 3984))),
       ('web', (62859, 31081, 3487, 34496, 36829))]),
    ('a note emptied on one of two rows flipped between two computers forever',
     717, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (58310, 32163, 3385, 16385, 21073)), ('desk', 'A', (64512, 21699, 2834, 33625, 8665)),
       ('sync', 'B', 'merge', None), ('desk', 'A', (24793, 30339, 34336, 4753, 60104)),
       ('sync', 'A', 'up', None), ('web', (40517, 44703, 31985, 16941, 48285)),
       ('desk', 'B', (47665, 50123, 16627, 25401, 10260))]),
    ('kept notes in another order flipped between two computers forever',
     74, {'has_page': False, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('web', (45118, 45319, 65119, 40046, 4883)), ('sync', 'A', 'down', None),
       ('desk', 'A', (209, 28057, 54787, 57181, 6650)), ('desk', 'A', (5344, 59649, 58718, 62795, 5369)),
       ('sync', 'B', 'down', None)]),
    ('after a website revert on one row, two computers swapped their notes on every sync',
     19838, {'has_page': True, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('desk', 'B', (43934, 31496, 8140, 2404, 17319)), ('sync', 'A', 'merge', None),
       ('sync', 'B', 'merge', None), ('desk', 'A', (15253, 52563, 62986, 24496, 4843)),
       ('desk', 'A', (61478, 34109, 10832, 1806, 11420)), ('sync', 'A', 'merge', None),
       ('sync', 'B', 'down', None), ('web', (52778, 20725, 59027, 59816, 40672)), ('sync', 'B', 'down', None),
       ('web', (5291, 31198, 27649, 50301, 55943))]),
    ('a record whose row another computer had moved made a second Download create an entry',
     7550, {'has_page': False, 'max_rows': 2, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('sync', 'A', 'merge', None), ('desk', 'A', (47527, 42504, 30864, 19285, 59613)),
       ('sync', 'B', 'merge', None), ('desk', 'B', (45242, 33992, 38285, 62589, 9432)),
       ('desk', 'B', (56656, 46377, 23005, 61260, 12800)), ('sync', 'B', 'merge', None),
       ('sync', 'A', 'merge', None), ('migrate',), ('desk', 'A', (24560, 9137, 31008, 34587, 58736)),
       ('sync', 'A', 'up', None), ('sync', 'B', 'down', None), ('sync', 'B', 'down', None)]),
    ('a website duplicate brought a moved entry back and a second Download added another',
     878, {'has_page': False, 'max_rows': None, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False, 'upgrade': 14},
     [('sync', 'B', 'up', None), ('web', (18204, 56256, 33259, 22285, 23586)), ('sync', 'A', 'merge', None),
       ('desk', 'A', (56839, 51893, 21777, 14580, 29087)), ('desk', 'A', (46644, 56980, 39490, 60545, 9686)),
       ('sync', 'B', 'up', None), ('sync', 'A', 'up', None),
       ('desk', 'A', (35500, 13257, 50417, 32995, 51352)), ('web', (32094, 37733, 2548, 62787, 62150)),
       ('sync', 'B', 'merge', None), ('sync', 'A', 'down', None),
       ('web', (32581, 45045, 60692, 52444, 28667)), ('web', (4415, 6840, 9307, 24235, 61747)),
       ('desk', 'A', (1447, 42587, 40648, 60648, 48229)), ('sync', 'A', 'merge', None),
       ('web', (38404, 28471, 60499, 32847, 37047)), ('sync', 'B', 'merge', None)]),
    ('a second website row of a folio folded into another entry on the next Download',
     14427, {'has_page': True, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'A', (23281, 53233, 60751, 64029, 7322)), ('web', (52506, 2491, 25910, 4427, 39973)),
       ('web', (42930, 5745, 51262, 22768, 9623)), ('desk', 'A', (26342, 51598, 53880, 23680, 22645)),
       ('desk', 'B', (14875, 3335, 10556, 15701, 51996)), ('sync', 'B', 'merge', None),
       ('sync', 'A', 'up', None), ('desk', 'B', (42206, 8796, 36864, 49515, 15752)),
       ('sync', 'B', 'up', None), ('web', (36037, 8753, 61272, 26861, 35318)),
       ('sync', 'B', 'up', (7, 'same_desktop_edit', (36663, 49749, 9192, 43327, 30958))),
       ('web', (6238, 11139, 55407, 12725, 14252)), ('sync', 'A', 'down', None),
       ('web', (25554, 20934, 1689, 11191, 44397)), ('sync', 'A', 'up', None), ('sync', 'B', 'down', None),
       ('sync', 'B', 'down', None)]),
    ('the entry a second row folds into changed once the first fold changed its note',
     14361, {'has_page': False, 'max_rows': 2, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('sync', 'B', 'merge', None), ('web', (38638, 37825, 7165, 55985, 40625)), ('sync', 'A', 'down', None),
       ('web', (17812, 53052, 7245, 58730, 20288)), ('sync', 'B', 'merge', None),
       ('desk', 'B', (7207, 6003, 4015, 55777, 41713)), ('web', (53845, 21000, 59244, 12326, 2467)),
       ('desk', 'B', (36036, 7538, 19326, 15376, 8197)), ('sync', 'B', 'up', None),
       ('desk', 'B', (26324, 41837, 3050, 16751, 32023)), ('desk', 'B', (53520, 10260, 6621, 4309, 8919)),
       ('sync', 'A', 'merge', None), ('sync', 'B', 'up', None), ('web', (45665, 52720, 23719, 43623, 62998)),
       ('sync', 'B', 'down', None)]),
    ('an item created from a later row was the better pair for an earlier one',
     17212, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': False, 'upgrade': 20},
     [('sync', 'A', 'up', None), ('desk', 'A', (30655, 54628, 25768, 62223, 36253)),
       ('sync', 'A', 'down', None), ('web', (485, 30436, 31537, 549, 30904)),
       ('sync', 'A', 'up', (15, 'web', (62863, 48164, 8322, 59113, 1137))), ('sync', 'A', 'merge', None),
       ('desk', 'A', (782, 36710, 17888, 56321, 31816)),
       ('sync', 'A', 'up', (14, 'other_desktop_pass', (44832, 40451, 16310, 37352, 48378))),
       ('desk', 'B', (40788, 62727, 53985, 34959, 46441)), ('sync', 'B', 'up', None),
       ('desk', 'B', (13995, 45948, 17868, 4602, 55230)),
       ('sync', 'A', 'down', (22, 'other_desktop_pass', (55202, 11175, 62374, 45674, 54117))),
       ('desk', 'A', (1466, 60108, 44136, 47722, 9625)), ('web', (19507, 45539, 31790, 22529, 42408)),
       ('sync', 'B', 'merge', None), ('web', (1631, 27089, 56726, 49273, 27347)), ('sync', 'B', 'up', None),
       ('sync', 'A', 'down', None), ('account', 'A', 'u2'), ('sync', 'B', 'up', None),
       ('sync', 'B', 'merge', None), ('desk', 'B', (2066, 10004, 20906, 64334, 6057)), ('account', 'A', 'u1'),
       ('sync', 'B', 'merge', None), ('sync', 'A', 'up', None),
       ('desk', 'B', (22827, 29418, 61351, 52973, 55404)), ('web', (39589, 11292, 64277, 25370, 60393)),
       ('sync', 'B', 'down', (15, 'raise_before', (42260, 33361, 34048, 25108, 57664)))]),
    ('an item brought into the list by a same-name row was the better pair for its own row',
     72, {'has_page': True, 'max_rows': None, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (5004, 2429, 29702, 21531, 45823)), ('sync', 'B', 'merge', None),
       ('sync', 'A', 'merge', None), ('desk', 'A', (35407, 12702, 39909, 22165, 53456)),
       ('web', (45555, 8383, 41956, 11931, 23356)), ('desk', 'A', (49063, 57378, 11269, 53496, 43858)),
       ('sync', 'B', 'down', None), ('web', (38986, 16737, 16263, 21081, 8664)),
       ('desk', 'A', (39259, 44880, 60366, 46741, 22246)), ('sync', 'A', 'merge', None),
       ('desk', 'A', (40399, 33607, 14733, 52965, 61299)), ('desk', 'A', (50640, 689, 43841, 40485, 21217)),
       ('desk', 'A', (48331, 59907, 13195, 44347, 16778)), ('sync', 'A', 'up', None),
       ('desk', 'B', (12187, 33874, 5486, 23466, 55535)), ('desk', 'B', (6871, 2958, 37990, 29890, 55266)),
       ('desk', 'B', (42633, 10872, 57818, 29692, 61852)), ('sync', 'B', 'up', None),
       ('desk', 'B', (55784, 128, 798, 54585, 6385)), ('sync', 'B', 'merge', None),
       ('web', (3852, 3207, 32429, 48647, 35642)), ('sync', 'A', 'merge', None),
       ('web', (29133, 828, 57773, 55151, 40026)), ('sync', 'B', 'down', None)]),
    ('a Download replaced a moved entry whose ::row:: key it reused',
     291, {'has_page': True, 'max_rows': 2, 'p_inject': 0.0, 'past_end_raises': False, 'page_lag': False, 'upgrade': 0},
     [('sync', 'A', 'down', None), ('sync', 'A', 'up', None),
       ('desk', 'B', (18732, 10690, 8331, 42376, 6013)), ('desk', 'A', (1896, 32365, 7121, 1098, 50907)),
       ('web', (31308, 29343, 28812, 26220, 50800)), ('web', (50316, 18804, 65287, 28457, 23758)),
       ('desk', 'B', (23042, 15827, 26758, 3356, 50900)), ('sync', 'B', 'up', None),
       ('desk', 'A', (62899, 50921, 38280, 15833, 50330)), ('web', (12385, 18782, 61031, 43917, 42629)),
       ('web', (20751, 43484, 49818, 35822, 12888)), ('sync', 'A', 'down', None),
       ('desk', 'A', (57477, 15430, 34422, 26914, 49020)), ('desk', 'A', (58548, 20086, 22658, 11438, 17798)),
       ('sync', 'A', 'up', None), ('desk', 'A', (32426, 11671, 44759, 49841, 39435)),
       ('web', (26190, 10033, 19469, 59989, 48234)), ('sync', 'A', 'down', None),
       ('desk', 'A', (47439, 51690, 21604, 1685, 45199)), ('sync', 'A', 'merge', None),
       ('desk', 'A', (37372, 223, 20104, 44388, 65512)), ('sync', 'A', 'merge', None)]),
    ('two entries named one row after it moved out of a list in the Trash',
     1439, {'has_page': False, 'max_rows': 2, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': True, 'upgrade': 14},
     [('web', (24058, 22169, 18941, 61649, 46548)), ('sync', 'B', 'merge', None),
       ('desk', 'B', (11059, 40564, 40864, 42344, 12090)), ('sync', 'B', 'up', None),
       ('sync', 'B', 'up', None), ('web', (34711, 40619, 64990, 6790, 22203)), ('sync', 'B', 'down', None),
       ('web', (6406, 64052, 1605, 61847, 34411)), ('desk', 'A', (31609, 64043, 9214, 39871, 13541)),
       ('web', (36973, 15339, 24770, 17610, 4386)), ('sync', 'A', 'down', None),
       ('desk', 'A', (29642, 27509, 54509, 42692, 54560)), ('desk', 'B', (38197, 43219, 40254, 51929, 37754)),
       ('desk', 'A', (13694, 7768, 54197, 33591, 26834)), ('sync', 'A', 'merge', None),
       ('web', (27162, 26897, 17084, 26915, 40173)), ('sync', 'B', 'merge', None),
       ('desk', 'A', (8575, 30503, 49202, 9016, 17030)), ('desk', 'A', (27160, 18207, 5203, 57047, 14883)),
       ('desk', 'B', (33373, 17912, 46032, 2238, 24747)), ('sync', 'A', 'merge', None),
       ('sync', 'B', 'up', None), ('web', (61441, 58263, 34585, 40753, 30977)), ('account', 'A', 'u2'),
       ('sync', 'B', 'down', None), ('sync', 'A', 'up', None), ('account', 'A', 'u1'),
       ('sync', 'A', 'down', None), ('desk', 'A', (55359, 20461, 39164, 29828, 41785)),
       ('desk', 'B', (36381, 41446, 31062, 9175, 46320))]),
    ("a note difference on the row about to be moved did not hold the entry's other rows",
     243, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 12},
     [('sync', 'B', 'down', (21, 'other_desktop_pass', (39371, 569, 26649, 27882, 54075))),
       ('desk', 'B', (13573, 1881, 48812, 7183, 64156)), ('desk', 'A', (33640, 5450, 31952, 24641, 46161)),
       ('sync', 'A', 'merge', (24, 'session_lost', (8764, 49472, 50778, 29356, 26254))),
       ('web', (1599, 37779, 45225, 49973, 2669)), ('sync', 'B', 'merge', None),
       ('desk', 'B', (8077, 2020, 35987, 63386, 60947)), ('desk', 'B', (44641, 15324, 30902, 56944, 12215)),
       ('sync', 'B', 'merge', (21, 'session_lost', (2381, 7853, 47359, 59592, 29344))),
       ('long', 'B', (31849, 15353, 5649, 59273, 36336)), ('desk', 'B', (39559, 22028, 29384, 36454, 17906)),
       ('desk', 'A', (43233, 28863, 3425, 39720, 59530)),
       ('sync', 'B', 'up', (5, 'url_too_long', (64787, 38909, 88, 26224, 3369))),
       ('web', (63846, 24715, 50607, 44980, 12895)), ('sync', 'A', 'down', None),
       ('web', (38227, 6502, 10216, 25167, 36980)), ('web', (45922, 13588, 47873, 84, 12753)),
       ('desk', 'A', (42001, 63958, 30010, 11398, 58101)), ('desk', 'A', (6652, 52775, 31844, 8264, 6689)),
       ('web', (52242, 10136, 63649, 29380, 30035)), ('web', (23879, 17602, 31534, 17585, 65013)),
       ('sync', 'A', 'up', (19, 'other_desktop_pass', (31475, 621, 41924, 32265, 48461))),
       ('account', 'B', 'u2'), ('sync', 'B', 'up', None), ('account', 'B', 'u1'),
       ('sync', 'B', 'merge', None), ('web', (1954, 46392, 19498, 57148, 55128)),
       ('desk', 'B', (18688, 40982, 11238, 25701, 61306)), ('desk', 'B', (25767, 38569, 29342, 36639, 13901)),
       ('web', (12635, 56270, 27070, 17243, 22988)), ('desk', 'B', (15040, 44623, 59307, 45382, 8779)),
       ('sync', 'B', 'up', None)]),
    ("an unrelated orphan's difference held one upload's writes and not the next one's",
     7252, {'has_page': False, 'max_rows': None, 'p_inject': 0.0, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('web', (56470, 17753, 4645, 54737, 47492)), ('sync', 'A', 'up', None), ('sync', 'A', 'down', None),
       ('web', (43003, 12200, 29613, 37252, 37632)), ('sync', 'B', 'down', None),
       ('web', (33120, 22109, 10743, 4435, 22279)), ('desk', 'A', (2971, 12802, 54179, 48925, 2159)),
       ('desk', 'A', (39129, 49306, 51511, 16036, 1345)), ('sync', 'A', 'up', None),
       ('sync', 'B', 'down', None), ('desk', 'B', (11527, 61054, 38872, 21555, 36414)),
       ('desk', 'B', (27191, 49562, 16774, 22009, 17458)), ('sync', 'B', 'merge', None),
       ('web', (9713, 6229, 26870, 31462, 9474)), ('desk', 'B', (28312, 52306, 23781, 26542, 24871)),
       ('sync', 'B', 'up', None)]),
    ('the website edit of a moved row in a list in the Trash reached the entry only on a second Merge',
     400, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False, 'upgrade': 18},
     [('web', (64746, 14856, 57128, 62997, 40065)), ('web', (43843, 33186, 4264, 65477, 33968)),
       ('web', (13626, 64734, 47455, 57670, 2063)), ('web', (44276, 10266, 64364, 62710, 11883)),
       ('desk', 'B', (3013, 65448, 65158, 13100, 10613)), ('desk', 'A', (50435, 5630, 30048, 28768, 41228)),
       ('web', (10476, 30599, 45353, 15691, 37170)), ('desk', 'B', (40336, 48348, 15731, 36933, 52857)),
       ('desk', 'B', (55135, 29067, 40511, 4211, 34738)), ('sync', 'B', 'up', None),
       ('desk', 'A', (37336, 1699, 41337, 42607, 50754)), ('sync', 'B', 'up', None),
       ('web', (60282, 12781, 52728, 24595, 40220)), ('sync', 'B', 'up', None), ('restart', 'A'),
       ('desk', 'B', (45931, 64823, 49635, 57175, 28982)), ('sync', 'A', 'up', None),
       ('sync', 'A', 'up', None), ('sync', 'A', 'merge', None), ('web', (6458, 50988, 61601, 604, 63212)),
       ('sync', 'B', 'merge', None), ('sync', 'A', 'merge', None),
       ('desk', 'A', (14140, 27486, 17630, 50603, 42651)), ('web', (39548, 64697, 27105, 43001, 45026)),
       ('sync', 'A', 'merge', None)]),
    ('text spread one hop per round through several rows of one manuscript (more than 5 rounds)',
     31, {'has_page': False, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': True, 'upgrade': 0},
     [('migrate',), ('desk', 'B', (28896, 6313, 14003, 11313, 57128)), ('sync', 'A', 'up', None),
       ('web', (51948, 43375, 23072, 47077, 35214)), ('web', (14892, 48771, 35344, 60443, 28544)),
       ('sync', 'A', 'merge', None), ('desk', 'A', (7464, 41572, 34400, 39441, 19290)),
       ('desk', 'A', (55133, 44254, 22005, 57014, 42650)), ('desk', 'A', (45076, 28382, 52037, 1054, 53196)),
       ('web', (10773, 29206, 26105, 61921, 60801)),
       ('sync', 'A', 'down', (28, 'raise_before', (17885, 9451, 56463, 3910, 11426))),
       ('sync', 'B', 'merge', None)]),
    ("a removed membership held its entry's note on the other rows in the pass after its removal",
     1780, {'has_page': False, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('web', (52162, 16056, 56628, 23049, 36958)), ('desk', 'B', (35077, 6574, 5744, 22593, 24341)),
       ('sync', 'A', 'up', (17, 'session_lost', (29328, 30805, 51896, 14523, 45648))),
       ('web', (2242, 56660, 43465, 3847, 24407)), ('desk', 'B', (52764, 8013, 2579, 2026, 50101)),
       ('sync', 'B', 'merge', (25, 'other_desktop_pass', (24514, 43818, 59736, 45013, 10818))),
       ('desk', 'B', (33283, 51414, 37085, 5436, 19272)), ('web', (25425, 34976, 59360, 38266, 64176)),
       ('desk', 'B', (59845, 40825, 39839, 16422, 12640)), ('sync', 'B', 'up', None),
       ('desk', 'B', (54295, 5927, 1596, 61369, 26683)), ('web', (18582, 22917, 62398, 32280, 6170)),
       ('web', (15520, 4185, 33419, 56573, 62608)),
       ('sync', 'A', 'up', (3, 'web', (3736, 7793, 38375, 25393, 55690))),
       ('sync', 'B', 'up', (24, 'session_lost', (62056, 40279, 16694, 3181, 41449))),
       ('desk', 'B', (57664, 28564, 61406, 21363, 30772)), ('sync', 'A', 'merge', None),
       ('web', (3543, 26613, 22726, 20803, 45369)), ('web', (56443, 39931, 32079, 33905, 26118)),
       ('web', (51570, 25847, 8636, 12386, 3112)), ('sync', 'B', 'merge', None), ('sync', 'A', 'merge', None),
       ('web', (11755, 54339, 1578, 57116, 39342)), ('web', (11128, 13927, 21004, 13918, 38570)),
       ('desk', 'B', (56620, 4716, 15946, 52576, 44141)), ('sync', 'B', 'merge', None),
       ('web', (27751, 17576, 63988, 10781, 23634)), ('sync', 'A', 'down', None), ('sync', 'A', 'up', None),
       ('desk', 'B', (46922, 3949, 42184, 2385, 10074)), ('sync', 'B', 'up', None),
       ('sync', 'A', 'merge', None), ('web', (47602, 31192, 26855, 58633, 46879)),
       ('long', 'B', (16485, 30251, 65207, 40032, 39849)), ('sync', 'B', 'up', None),
       ('web', (9240, 31646, 26785, 10541, 47742)), ('desk', 'B', (38804, 57945, 59913, 65511, 2712)),
       ('web', (15310, 31634, 59641, 42326, 4696)), ('desk', 'A', (13420, 54655, 48389, 46797, 24074)),
       ('web', (45038, 25671, 14150, 19815, 55857)), ('desk', 'B', (46327, 39911, 30724, 16734, 37939)),
       ('sync', 'A', 'up', None), ('desk', 'B', (9686, 58665, 13783, 1627, 5121)),
       ('desk', 'B', (59100, 9899, 7812, 41298, 27725)), ('web', (40384, 28431, 58671, 48032, 14333)),
       ('sync', 'B', 'merge', None), ('web', (29195, 38339, 62332, 61373, 30470)),
       ('web', (44377, 7809, 28234, 34128, 3639)), ('sync', 'B', 'up', None)]),
    ("a same-name list's row paired with another entry once its own entry held its own list's row",
     1759, {'has_page': False, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('sync', 'B', 'up', None), ('web', (1743, 45120, 59907, 57757, 43112)), ('sync', 'B', 'merge', None),
       ('web', (4399, 6456, 49344, 27825, 33731)), ('web', (31442, 48524, 63549, 62723, 10789)),
       ('web', (11169, 29338, 29409, 14747, 10864)), ('sync', 'A', 'merge', None),
       ('desk', 'A', (34074, 16998, 63251, 15126, 144)), ('desk', 'A', (36024, 51149, 65235, 36621, 28877)),
       ('sync', 'B', 'merge', None), ('sync', 'A', 'merge', None), ('web', (48529, 26015, 23232, 4284, 54610)),
       ('web', (59548, 6490, 18225, 458, 21400)), ('desk', 'B', (43639, 49761, 9818, 14632, 168)),
       ('desk', 'B', (53624, 46632, 4876, 38829, 44285)),
       ('sync', 'B', 'up', (13, 'url_too_long', (19695, 63529, 10578, 32123, 9901))),
       ('sync', 'A', 'merge', None)]),
    ("an orphan recorded in its list's own cloud list, not located, was moved from that list into it",
     3770, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 16},
     [('account', 'B', 'u2'), ('web', (23703, 21568, 52417, 63400, 60004)), ('account', 'B', 'u1'),
       ('sync', 'A', 'up', (2, 'web_between_pages', (24208, 43805, 55308, 10215, 11964))),
       ('web', (41479, 9485, 56, 6321, 29796)), ('sync', 'A', 'up', None),
       ('long', 'A', (13226, 46167, 53301, 10121, 15779)), ('sync', 'A', 'down', None),
       ('sync', 'A', 'down', None),
       ('sync', 'A', 'up', (9, 'raise_before', (39375, 18163, 20239, 33018, 43671))),
       ('web', (6840, 30101, 51372, 33417, 3976)), ('desk', 'B', (31170, 41479, 55716, 56723, 20624)),
       ('sync', 'B', 'merge', (2, 'web_between_pages', (14481, 51768, 34496, 5590, 5920))),
       ('sync', 'B', 'up', None), ('desk', 'A', (9425, 49673, 19231, 15782, 49429)),
       ('desk', 'A', (50484, 28212, 27224, 46768, 831)), ('sync', 'A', 'merge', None),
       ('desk', 'A', (27475, 57382, 63336, 27993, 44092)), ('desk', 'A', (24812, 39164, 6256, 12267, 63018)),
       ('sync', 'A', 'up', (7, 'session_lost', (4663, 62412, 12373, 24980, 56077)))]),
    ("a same-name list's row went to another entry of one folio once the entry it reached first "
     "held its own list's row (moved there from the list it was merged out of)",
     4358, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('desk_rename', 'B', (35017, 1858, 4835, 29122, 44445)),
      ('desk', 'A', (48746, 29481, 14145, 6357, 36817)), ('sync', 'A', 'merge', None),
      ('sync', 'B', 'up', None), ('web', (53434, 27218, 26940, 27944, 64906)),
      ('web', (44352, 47899, 54586, 59829, 37010)), ('sync', 'A', 'down', None), ('sync', 'B', 'down', None),
      ('desk', 'A', (29990, 62308, 41088, 16571, 42824)), ('desk', 'B', (35476, 17563, 16611, 22522, 45025)),
      ('desk', 'B', (18859, 2893, 62569, 42547, 54382)), ('sync', 'B', 'up', None),
      ('web', (63852, 17886, 10900, 39268, 24913)),
      ('desk_rename', 'A', (27491, 10572, 44753, 48602, 30299)), ('sync', 'A', 'up', None),
      ('account', 'A', 'u2'),
      ('sync', 'A', 'merge', (13, 'same_desktop_edit', (56694, 39368, 27580, 19178, 15523))),
      ('account', 'A', 'u1'), ('sync', 'A', 'merge', None), ('desk', 'A', (41784, 58354, 8052, 6929, 12275)),
      ('sync', 'A', 'up', None), ('desk', 'A', (21050, 59352, 28449, 38070, 62100)),
      ('desk', 'A', (5292, 11794, 23200, 36613, 14501)), ('desk', 'A', (40692, 15655, 20529, 9028, 7631)),
      ('sync', 'B', 'merge', None), ('desk', 'A', (57953, 58995, 18276, 24636, 13131)),
      ('sync', 'A', 'up', None), ('web', (43197, 52195, 39850, 39488, 99)), ('sync', 'B', 'merge', None),
      ('web', (37824, 1976, 60854, 4639, 44485)), ('web_rename', (64888, 11673, 18798, 9523, 2905)),
      ('desk', 'A', (65462, 46037, 3896, 29185, 61264)), ('desk', 'A', (15685, 53968, 46973, 56584, 46976)),
      ('sync', 'B', 'merge', None), ('web', (41868, 45567, 6446, 40073, 52989)),
      ('sync', 'A', 'merge', (27, 'raise_after', (20494, 63134, 32997, 27245, 29846))),
      ('web', (62194, 52885, 56281, 7597, 32928)), ('desk', 'A', (21236, 15023, 7798, 65415, 17692)),
      ('sync', 'B', 'down', None), ('sync', 'B', 'up', None), ('sync', 'A', 'merge', None)]),
    ("a same-name list's row of a page went to the whole folio's entry once that entry held its note, "
     "not to the page's own entry",
     11077, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('sync', 'B', 'up', None), ('web', (8817, 43462, 42293, 39102, 62111)),
      ('web', (59612, 55118, 3729, 30776, 10269)), ('desk', 'A', (28418, 13852, 23251, 35949, 30797)),
      ('sync', 'A', 'merge', None), ('web', (24658, 51597, 18132, 11299, 10377)),
      ('web', (32131, 61505, 48975, 30911, 3845)), ('sync', 'B', 'down', None), ('sync', 'A', 'merge', None),
      ('desk_rename', 'B', (58130, 3288, 40361, 2542, 44858)), ('sync', 'B', 'down', None),
      ('desk', 'A', (62366, 7590, 45014, 61873, 37598)), ('desk', 'A', (3247, 18543, 21444, 11510, 43000)),
      ('sync', 'A', 'down', (3, 'web_rename', (18707, 36144, 16070, 33751, 15379))),
      ('desk', 'B', (37052, 49207, 15448, 7687, 33798)), ('desk', 'A', (7441, 19972, 47944, 57701, 48887)),
      ('sync', 'A', 'up', None), ('web', (27311, 5205, 40138, 26087, 7734)),
      ('web', (47832, 16114, 22540, 3824, 21564)), ('sync', 'A', 'merge', None),
      ('desk', 'A', (1405, 36058, 48873, 48079, 21627)),
      ('sync', 'B', 'up', (1, 'other_desktop_pass', (62969, 27080, 55291, 7365, 61102))),
      ('sync', 'B', 'down', None), ('sync', 'B', 'up', None),
      ('desk', 'B', (61139, 51062, 33263, 58552, 42574)), ('desk', 'B', (21217, 25576, 55317, 21859, 19557)),
      ('web', (60354, 3132, 19166, 25446, 3609)), ('web', (42385, 33997, 36729, 4599, 1816)),
      ('sync', 'B', 'up', None), ('web_rename', (63093, 3413, 52554, 31471, 22154)),
      ('desk', 'A', (36383, 44278, 29542, 49322, 58659)), ('desk', 'A', (37663, 14922, 60402, 5499, 41233)),
      ('desk', 'B', (28329, 5366, 11900, 10435, 51572)), ('desk', 'A', (39043, 35834, 10784, 31395, 11111)),
      ('web', (14217, 46494, 59707, 62981, 16009)), ('sync', 'A', 'merge', None),
      ('web', (44835, 51194, 5474, 26948, 34281)), ('sync', 'B', 'up', None),
      ('web', (56867, 12106, 14616, 10850, 22337)), ('web', (29458, 6191, 19422, 16282, 18859)),
      ('sync', 'B', 'down', (24, 'web_between_pages', (43243, 28233, 32342, 2249, 24759))),
      ('desk', 'B', (60223, 10711, 28689, 32579, 12081)), ('desk', 'B', (13017, 11459, 10907, 65477, 12803)),
      ('sync', 'B', 'up', None), ('sync', 'B', 'merge', None)]),
    # A batch insert that commits and then fails: nothing of it may be inserted again in that pass
    # (each case broke invariant 4 on an engine that retried such a batch one row at a time).
    ('a batch whose answer was lost after it committed was inserted again, row by row',
     1883, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (8581, 64748, 56079, 18071, 56577)), ('desk', 'B', (48110, 40511, 3201, 16852, 30760)),
      ('sync', 'B', 'up', (2, 'raise_after', (22182, 41880, 27129, 44716, 14111)))]),
    ('a batch answered with a gateway 504 after it committed was inserted again',
     3329, {'has_page': True, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk', 'B', (41318, 41771, 42592, 24779, 60326)), ('desk', 'B', (46694, 33531, 848, 478, 7086)),
      ('sync', 'B', 'up', (10, 'api_error', (33674, 34131, 22158, 4860, 16634)))]),
    ('a batch answered with PGRST111 after it committed was inserted again',
     2742, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False, 'upgrade': 0},
     [('desk', 'A', (12985, 22937, 35201, 15278, 51992)), ('desk', 'A', (32485, 17432, 21126, 61327, 52528)),
      ('sync', 'A', 'merge', (5, 'api_error', (59488, 48933, 5041, 47272, 43114)))]),
    ('a batch that wrote nothing was retried row by row, and the second entry of one folio was taken for a '
     'duplicate of the first',
     6389, {'has_page': False, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False,
            'upgrade': 0},
     [('desk', 'A', (52764, 28401, 39288, 37455, 28580)), ('desk', 'A', (30433, 41913, 11424, 58992, 35037)),
      ('sync', 'A', 'up', (21, 'api_error', (47592, 53775, 51533, 19122, 37878)))]),
    # A list renamed on the website or on a desktop (each broke invariant 10 on the engine whose
    # upload renamed the website's list back):
    ('a Download left a list renamed on the website under its old name',
     4, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('sync', 'A', 'up', None), ('web_rename', (23348, 20315, 7767, 17937, 54587)), ('sync', 'A', 'down', None)]),
    ("an upload wrote a list's old name back over the website's rename",
     16, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('sync', 'B', 'down', None), ('web_rename', (59256, 36963, 14617, 38210, 56426))]),
    ("a Download gave a list renamed on the desktop, not yet uploaded, the website's name",
     38, {'has_page': True, 'max_rows': None, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False, 'upgrade': 0},
     [('sync', 'B', 'merge', None), ('desk_rename', 'B', (17863, 10066, 21035, 26839, 22811)),
      ('sync', 'B', 'down', None)]),
    # Found once invariant 10 was checked against the harness's own record of desktop renames
    # rather than the engine's mark (one mark stood for both a rename here and a colour kept
    # at the first Download, and hid both of these):
    ("a first Download that kept this computer's colour for a list held back a website rename of it",
     1450, {'has_page': True, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False,
            'upgrade': 0},
     [('desk', 'A', (8323, 16275, 40837, 44815, 22096)), ('sync', 'A', 'down', None),
      ('web_rename', (60212, 2094, 1510, 58662, 48072)), ('sync', 'A', 'down', None)]),
    ("the default list took the website's General list at a Download and kept a name it had already sent",
     177, {'has_page': True, 'max_rows': 2, 'p_inject': 0.1, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('desk_rename', 'B', (1770, 38695, 16830, 18797, 14704)), ('sync', 'A', 'up', None),
      ('web', (4119, 30172, 45192, 22056, 4696)), ('sync', 'B', 'up', None),
      ('web', (29959, 55317, 45872, 18370, 53847)), ('web', (40291, 3555, 48530, 27660, 46256)),
      ('sync', 'B', 'down', None)]),
    # Found once renames were generated: a website paste that put back the note a desktop last saw
    # on a row is replaced there by that desktop's edit (invariant 1 retires what the paste revived):
    ('a note pasted back onto a row was replaced there by the desktop edit that had replaced it',
     412, {'has_page': False, 'max_rows': 2, 'p_inject': 0.1, 'past_end_raises': False, 'page_lag': False, 'upgrade': 0},
     [('migrate',), ('desk', 'A', (43020, 37478, 63093, 24030, 33365)),
      ('sync', 'A', 'merge', (3, 'web_rename', (34189, 56224, 46855, 41088, 2204))),
      ('desk', 'B', (48026, 17584, 63357, 8050, 46235)), ('sync', 'B', 'merge', None),
      ('web', (9001, 50677, 36323, 2377, 40998)), ('desk', 'B', (6559, 2392, 35662, 2192, 53118)),
      ('web', (62280, 12650, 20407, 44875, 43614)), ('desk', 'B', (31825, 33649, 15444, 15282, 4627)),
      ('sync', 'B', 'merge', None),
      ('sync', 'A', 'down', (22, 'web_between_pages', (24188, 31557, 3564, 50125, 24444))),
      ('desk', 'B', (10790, 41993, 31662, 40651, 38597)), ('desk', 'A', (20172, 14027, 46191, 36093, 57795)),
      ('desk', 'A', (37718, 30117, 21181, 27101, 17619)), ('sync', 'B', 'merge', None),
      ('web', (44353, 13324, 6355, 13947, 5750)), ('desk', 'A', (35832, 13736, 27289, 4254, 3925)),
      ('sync', 'A', 'up', (16, 'web_rename', (55051, 45246, 18077, 44677, 53706))), ('sync', 'B', 'down', None),
      ('web', (17606, 7480, 16584, 41056, 488)), ('desk', 'B', (11609, 785, 17263, 59630, 46007)),
      ('web', (3160, 4457, 48390, 13222, 18737)), ('web', (56600, 41045, 44484, 10634, 40926))]),
    # Found while the removal and move bookkeeping was built (each broke an invariant on a draft of it):
    ('an entry removed and put back in its list before the upload had its website row deleted anyway',
     120, {'has_page': True, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': False,
           'upgrade': 16},
     [('web_rename', (14827, 61000, 63433, 17129, 49772)), ('web', (54526, 53118, 26151, 58583, 58947)),
      ('desk', 'B', (34175, 3531, 60565, 24386, 62474)), ('desk', 'B', (31084, 51660, 14927, 23076, 30633)),
      ('desk', 'B', (25664, 28079, 42234, 9953, 29651)), ('desk', 'B', (50292, 34407, 38763, 51718, 22469)),
      ('prompt', 'A', (23372, 20370, 7308, 10748, 22544)), ('sync', 'A', 'up', None),
      ('signout', 'A', (10084, 14675, 4349, 19941, 59303)), ('sync', 'B', 'up', None), ('sync', 'B', 'down', None),
      ('account', 'A', 'u1'), ('desk', 'B', (30846, 49404, 49354, 38515, 13172)),
      ('desk', 'B', (59368, 27585, 56925, 25093, 44822)), ('desk', 'B', (42762, 47257, 64866, 5527, 61812)),
      ('ui', 'A', (False, False, False, False, False)), ('sync', 'B', 'up', None),
      ('desk', 'B', (44319, 20064, 27060, 1547, 14077)), ('desk', 'B', (65437, 24045, 26616, 6519, 32016)),
      ('sync', 'B', 'merge', (9, 'web_between_pages', (6270, 47403, 40720, 47306, 52881)))]),
    ("an entry made again under its key as another folio was given the old folio's row",
     9996, {'has_page': False, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': True,
            'upgrade': 0},
     [('desk', 'A', (50268, 61828, 41248, 18906, 49789)), ('sync', 'A', 'merge', None),
      ('desk', 'A', (47187, 28556, 60435, 65450, 22881)), ('desk', 'A', (57985, 22842, 55747, 63472, 13151))]),
    ('a Download left out, in every list, the row of a removal that another computer had moved elsewhere, '
     'so a second Merge changed things again',
     1891, {'has_page': True, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': False,
            'upgrade': 0},
     [('web', (2143, 6835, 53508, 51695, 61086)),
      ('sync', 'B', 'merge', (20, 'raise_before', (23668, 12895, 9027, 15409, 29035))), ('sync', 'A', 'merge', None),
      ('desk', 'B', (19455, 23741, 35183, 63124, 62585)), ('desk', 'A', (52372, 17606, 25692, 55483, 47890)),
      ('sync', 'A', 'merge', (18, 'web_between_pages', (56983, 49584, 40143, 59860, 18179))),
      ('sync', 'B', 'merge', None)]),
    ('a removal was dropped with no request on one answer that its row was gone (an anonymous confirmation)',
     2436, {'has_page': False, 'max_rows': None, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': True,
            'upgrade': 0},
     [('sync', 'B', 'up', (24, 'web', (38589, 14530, 60581, 38022, 21325))), ('account', 'B', 'u2'),
      ('sync', 'B', 'up', None), ('web_rename', (21173, 5581, 14243, 20869, 49604)),
      ('desk', 'A', (30235, 32122, 3220, 53872, 61052)), ('account', 'B', 'u1'),
      ('sync', 'A', 'up', (8, 'other_desktop_pass', (49653, 17635, 46062, 53137, 44847))),
      ('web', (14765, 6640, 59774, 47558, 5235)), ('web', (19778, 3490, 49269, 26173, 64430)),
      ('web', (43930, 2182, 5526, 556, 20693)), ('web', (54546, 46861, 44369, 34365, 9088)),
      ('sync', 'B', 'down', (10, 'raise_before', (61451, 41910, 26109, 45929, 6639))),
      ('desk', 'B', (57657, 34987, 60376, 37021, 40470)), ('sync', 'B', 'up', None),
      ('web', (23747, 48526, 10489, 2596, 23996)), ('sync', 'B', 'down', (3, 'web', (50847, 10781, 52581, 25079, 62163))),
      ('desk', 'B', (27195, 37191, 48054, 11022, 47866)),
      ('sync', 'B', 'up', (9, 'anon', (24255, 23779, 40875, 25636, 62219)))]),
    ('an entry a website row brought back into the list it was removed from kept its own row from its delete',
     192, {'has_page': True, 'max_rows': 2, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': False, 'upgrade': 0},
     [('web', (33742, 10070, 44440, 7846, 46101)), ('desk', 'A', (13022, 36920, 1937, 59778, 21436)),
      ('sync', 'A', 'merge', None), ('web', (20638, 61960, 50282, 30658, 29751)), ('sync', 'B', 'merge', None),
      ('desk', 'B', (48768, 64485, 52301, 11592, 39817)), ('web', (35064, 24523, 37250, 34264, 61015)),
      ('offline', 'B', 3), ('desk', 'B', (7875, 2138, 7321, 10218, 58145)),
      ('sync', 'B', 'up', (27, 'session_lost', (29171, 7294, 41456, 52898, 26978))), ('sync', 'B', 'down', None),
      ('sync', 'B', 'down', (19, 'url_too_long', (25787, 31129, 59333, 41534, 36669))), ('sync', 'B', 'down', None)]),
    # Found when the removal bookkeeping met keyset paging (each a false alarm of the harness, fixed there):
    ('a Download re-added an entry from a row the read returned before the website removed it; '
     'invariant 12 knew only the rows left at the fetch end, and took it for a row pending deletion',
     7122, {'has_page': False, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': False, 'page_lag': False,
            'upgrade': 0},
     [('sync', 'B', 'down', None), ('sync', 'A', 'merge', None), ('desk_rename', 'B', (15779, 60344, 58173, 45098, 35243)),
      ('sync', 'B', 'up', None), ('desk', 'B', (13880, 11486, 46248, 28784, 10069)),
      ('desk', 'A', (48240, 15491, 32374, 18742, 62142)),
      ('sync', 'A', 'up', (3, 'web_between_pages', (41123, 15432, 37000, 52105, 5910))),
      ('sync', 'B', 'merge', (19, 'url_too_long', (38591, 44793, 17009, 13657, 59586))),
      ('desk', 'B', (23643, 29272, 40634, 43535, 47978)),
      ('sync', 'B', 'down', (3, 'web_between_pages', (21122, 12616, 20403, 19325, 31685)))]),
    ("the website changed another account's list: its row failed row-level security and its note counted "
     "as user text (web_churn_same_count while a desktop was signed in as u2)",
     275, {'has_page': True, 'max_rows': 3, 'p_inject': 0.25, 'past_end_raises': True, 'page_lag': True, 'upgrade': 0},
     [('web', (63844, 44652, 32803, 13644, 51256)), ('sync', 'B', 'up', None), ('web', (53857, 62860, 23032, 49597, 25974)),
      ('desk', 'A', (300, 1704, 23845, 50946, 64078)), ('web', (4446, 44625, 46327, 37484, 34015)),
      ('sync', 'A', 'up', None), ('desk', 'B', (38005, 1776, 24230, 28559, 35739)),
      ('sync', 'B', 'merge', (15, 'session_lost', (33862, 23541, 58771, 49829, 39740))), ('account', 'B', 'u2'),
      ('sync', 'B', 'merge', None), ('account', 'A', 'u2'),
      ('sync', 'A', 'up', (14, 'web_churn_same_count', (60798, 6497, 36620, 45908, 543)))]),
]


def test_the_seed_block_holds_every_invariant():
    t0 = time.perf_counter()
    found = S.run_many(list(CI_SEEDS), CI_STEPS, 'current')
    took = time.perf_counter() - t0
    print(f'{len(CI_SEEDS)} seeds x {CI_STEPS} steps in {took:.1f}s')
    if found:
        seed = found[0][0]
        cfg = S.cfg_for(seed)
        ops = S.gen_ops(seed, CI_STEPS, cfg)
        small, v, w = S.shrink(seed, cfg, ops, 'current')
        pytest.fail(f'{len(found)} of {len(CI_SEEDS)} seeds break an invariant, first seeds '
                    f'{[f[0] for f in found[:10]]}\n{S.report(seed, cfg, small, v, w)}', pytrace=False)


@pytest.mark.parametrize('case', REGRESSION_CASES, ids=[c[0][:60] for c in REGRESSION_CASES])
def test_a_regression_case_replays_cleanly(case):
    _, seed, cfg, ops = case
    v, w = S.run_ops(seed, cfg, ops, 'current')
    assert v is None, S.report(seed, cfg, ops, v, w)


@pytest.mark.parametrize('inv', [1, 2, 4, 6])
def test_the_harness_catches_the_pre_2b_defects(inv):
    """The gate's own gate: on the engine before per-membership records, each invariant fires."""
    hits = [seed for seed in range(50)
            if (lambda v: v is not None and v.inv == inv)(S.run_seed(seed, 60, 'fixture',
                                                                     frozenset({inv, 'crash'}))[0])]
    assert hits, f'invariant {inv} never fired on the pre-2b engine in seeds 0-49'


# One rule of the removal bookkeeping reverted at a time, under the op mix that weights what it
# is for (list_sync_scenarios.gen_removal_mix: a removal during an upload, right after an insert or a
# move was answered; a Download after a removal; an entry put back in the list it left). On seeds
# 0-49 of that mix each of these fired in 26-41 seeds (2026-09-27); the rule they pin, and what fires:
REVERTED_RULES = {
    # finish_upload installs the copy without replaying the edits made meanwhile
    'no-replay': ((lists_manager.ListsManager, '_replay', lambda self, copy, journal: None), {14, 3, 11}),
    # a Download pairs, folds and re-adds rows waiting for their delete
    'no-download-suppression': ((lists_sync.ListsCloudSync, '_suppressed', lambda self, pass_, store: {}), {12}),
    # an entry put back in its list before its delete went does not get its website row back
    'put-back-keeps-no-row': ((lists_manager, '_restore_cloud_row', lambda state, item_id, list_id, ident: None),
                              {11}),
}


@pytest.mark.parametrize('rule', sorted(REVERTED_RULES))
def test_the_gate_catches_each_reverted_removal_rule(rule, monkeypatch):
    (owner, name, stand_in), invariants = REVERTED_RULES[rule]
    monkeypatch.setattr(owner, name, stand_in)
    found = S.run_many(list(range(50)), 60, 'current', mix='removals')
    fired = {inv for _, inv, _, _ in found}
    assert fired & invariants, f'with {rule}, no seed of 0-49 broke {sorted(invariants)} (fired: {sorted(fired)})'


# ---- the fake behaves like PostgREST where the engine depends on it

def _db(**kw):
    import random
    args = dict(has_page=True, max_rows=None, past_end_raises=False, page_lag=False)
    args.update(kw)
    db = S.FakeDB(random.Random(1), **args)
    web = S.FakeClient(db, 'web', 'u1')
    lst = web.table('user_lists').insert({'user_id': 'u1', 'name': 'L'}).execute().data[0]
    return db, web, lst


def test_the_fake_counts_before_the_range_and_caps_every_page():
    db, web, lst = _db(max_rows=2)
    web.table('list_items').insert([{'list_id': lst['id'], 'sys_id': str(n)} for n in range(5)]).execute()
    resp = (web.table('list_items').select('id', count='exact').eq('list_id', lst['id']).order('id')
            .range(0, 999).execute())
    assert len(resp.data) == 2 and resp.count == 5
    resp = web.table('list_items').select('id').in_('id', [r['id'] for r in db.tables['list_items']]).execute()
    assert len(resp.data) == 2 and resp.count is None


def test_the_fake_pages_by_row_id_under_its_row_cap():
    db, web, lst = _db(max_rows=2)
    web.table('list_items').insert([{'list_id': lst['id'], 'sys_id': str(n)} for n in range(5)]).execute()
    ids = sorted(r['id'] for r in db.tables['list_items'])

    def page():
        return web.table('list_items').select('id').eq('list_id', lst['id']).order('id').limit(1000)
    assert [r['id'] for r in page().execute().data] == ids[:2]
    assert [r['id'] for r in page().gt('id', ids[1]).execute().data] == ids[2:4]
    assert [r['id'] for r in page().gt('id', str(ids[3])).execute().data] == ids[4:]   # the id sent as text
    assert page().gt('id', ids[4]).execute().data == []
    with pytest.raises(S.APIError) as e:
        page().gt('id', 'x').execute()
    assert e.value.code == '22P02'


def test_the_fake_reads_tag_filters_as_jsonb():
    db, web, lst = _db()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'tags': ['a', 'ב"{,']}).execute()
    q = web.table('list_items').select('id')
    with pytest.raises(S.APIError) as e:
        q.contains('tags', ['a']).execute()
    assert e.value.code == '22P02'
    lit = '["ב\\"{,","a"]'
    assert len(web.table('list_items').select('id').contains('tags', lit).contained_by('tags', lit)
               .execute().data) == 1
    assert web.table('list_items').select('id').contained_by('tags', '["a"]').execute().data == []


def test_the_fake_hides_everything_without_a_session():
    db, web, lst = _db()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1'}).execute()
    anon = S.FakeClient(db, 'A', None)
    resp = anon.table('list_items').select('id', count='exact').execute()
    assert resp.data == [] and resp.count == 0
    with pytest.raises(S.APIError) as e:
        anon.table('list_items').insert({'list_id': lst['id'], 'sys_id': '2'}).execute()
    assert e.value.code == '42501'
    assert anon.table('list_items').update({'note': 'x'}).eq('id', 101).execute().data == []


def test_the_fake_answers_a_range_past_the_end_either_way():
    for raises in (False, True):
        db, web, lst = _db(past_end_raises=raises)
        web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1'}).execute()
        q = web.table('list_items').select('id', count='exact').eq('list_id', lst['id']).order('id').range(5, 9)
        if raises:
            with pytest.raises(S.APIError) as e:
                q.execute()
            assert e.value.code == 'PGRST103'
        else:
            assert q.execute().data == []


def test_the_fake_knows_whether_the_page_column_exists():
    db, web, lst = _db(has_page=False)
    with pytest.raises(S.APIError) as e:
        web.table('list_items').select('id, page').execute()
    assert e.value.code == '42703'
    with pytest.raises(S.APIError) as e:
        web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'page': '2'}).execute()
    assert e.value.code == 'PGRST204'
    db.migrate()
    web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'page': '2'}).execute()


def test_the_fake_refuses_a_query_string_over_the_gateway_limit():
    db, web, lst = _db()
    row = web.table('list_items').insert({'list_id': lst['id'], 'sys_id': '1', 'note': 'x' * 9000}).execute().data[0]
    with pytest.raises(S.APIError) as e:
        web.table('list_items').update({'note': 'y'}).eq('id', row['id']).eq('note', 'x' * 9000).execute()
    assert e.value.code == 414
    assert db.tables['list_items'][0]['note'] == 'x' * 9000
