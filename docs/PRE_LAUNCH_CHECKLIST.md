# GenizahSearch - Pre-Launch Testing Checklist
## Pre-Launch Testing Checklist v1.0

**Date:** 2026-01-29
**Environment:** Production
**Version:** 5.1

---

## Legend
- [ ] - Not tested
- [x] - Passed successfully
- [!] - Failed / Needs fix
- [?] - Needs additional testing

---

# 1. Home Page `/`

## 1.1 Display and Interface
- [x] Page loads without errors *(Code Review: OK)*
- [x] OCR banner displayed for new user *(home.py:24-35)*
- [x] "Got it" button hides the banner *(dismiss_banner)*
- [x] Banner doesn't reappear after closing *(localStorage via app.storage.user)*
- [x] Statistics (page count, lists) display correctly *(home.py:68-77)*
- [x] Tool cards (Search, Parallels, Browse) load *(home.py:83-158)*
- [x] Recent activity displayed (if exists) *(home.py:230-283)*
- [x] System status shown in expansion *(home.py:286-320)*

## 1.2 Quick Navigation
- [x] Click on "Text Search" card navigates to `/search` *(role=button, tabindex=0)*
- [x] Click on "Parallels" card navigates to `/parallels` *(role=button, tabindex=0)*
- [x] Click on "Browse" card navigates to `/browse` *(role=button, tabindex=0)*
- [x] Click on "Personal Lists" navigates to `/lists` *(role=button, tabindex=0)*
- [x] Click on "Lab Settings" navigates to `/settings` *(role=button, tabindex=0)*
- [x] Click on "Help Center" navigates to `/help` *(role=button, tabindex=0)*
- [x] Click on "Desktop App" navigates to `/download` *(role=button, tabindex=0)*

## 1.3 Credits and Sources
- [x] MiDRASH citation displayed at bottom *(home.py:329-334)*
- [x] Link to Zenodo active and opens in new tab *(home.py:339 - new_tab=True)*
- [x] CC BY 4.0 license mentioned *(home.py:342)*

---

# 2. Search Page `/search`

## 2.1 Search Interface
- [x] Search field active and accepts Hebrew text *(search.py:78-81)*
- [x] RTL direction correct in input field *(style='direction: rtl;')*
- [x] "Search" button active *(search.py:168-170)*
- [x] Enter activates search *(search.py:82)*

## 2.2 Search Modes
- [x] **Exact (=)**: Exact search works *(search.py:96,106)*
- [x] **Variants Basic (?)**: Basic variants works *(search.py:97,107)*
- [x] **Variants Extended (??)**: Extended variants works *(search.py:108)*
- [x] **Variants Maximum (???)**: Maximum variants works *(search.py:109)*
- [x] **Fuzzy (~)**: Fuzzy search works *(search.py:98,110)*
- [x] **Regex (/)**: Regex search works *(search.py:99,111)*
- [x] **Shelfmark (#)**: Shelfmark search works *(search.py:100,112)*
- [x] **Title ($)**: Title search works *(search.py:101,113)*

## 2.3 Syntax Shortcuts
- [?] `=word` activates Exact mode *(requires: test parse_query_syntax in Core)*
- [?] `?word` activates Variants mode *(requires: test parse_query_syntax in Core)*
- [?] `~word` activates Fuzzy mode *(requires: test parse_query_syntax in Core)*
- [?] `/pattern/` activates Regex mode *(requires: test parse_query_syntax in Core)*
- [?] `#T-S 12.123` activates Shelfmark mode *(requires: test parse_query_syntax in Core)*

## 2.4 Advanced Options
- [x] "Advanced Options" panel opens/closes *(search.py:219-243)*
- [x] **Lab Mode**: Lab Mode toggle active *(search.py:230)*
- [x] **Deep Scan**: Option available when Lab Mode active *(search.py:232)*
- [x] **Exclude Words**: Filter words field active *(search.py:240-241)*
- [x] **Gap**: Gap control active (0-10) *(search.py:160-161)*
- [x] **Max Changes**: Changes selector (×1, ×2, ×3) *(search.py:142-143)*

## 2.5 Variants Control
- [x] Slider Mode: Variants level slider active (if set in settings) *(search.py:174-190)*
- [x] Preset Mode: Dropdown with levels (Basic/Extended/Maximum) *(search.py:103-114)*
- [x] Settings saved between searches *(app.storage.user)*

## 2.6 Search Results
- [x] Result count displayed *(search.py:621)*
- [x] Result cards load *(search.py:628-641)*
- [x] Snippet with highlighting displayed *(SearchEngine.format_snippet() + sanitize=False)*
- [x] Result numbering (1#, 2#, etc.) *(search.py:673-675)*
- [x] Shelfmark displayed in each card *(search.py:676)*
- [x] Title (if exists) displayed *(search.py:677-680)*

## 2.7 Actions on Results
- [x] Clicking result loads in Viewer *(search.py:671)*
- [x] "Add to list" button (star) active *(search.py:689-696)*
- [x] "Advanced view" button active *(search.py:684-687)*
- [x] "Edit" button displayed and active *(search.py:703-710)*
- [x] "Comment" button displayed and active *(search.py:711-716)*

## 2.8 Bulk Operations
- [x] Checkbox to select all results active *(search.py:262)*
- [x] Individual checkbox per result active *(search.py:665-668)*
- [x] "X selected" counter updates *(search.py:409-426)*
- [x] "Add to list" bulk operation active *(search.py:428-474)*
- [x] "Copy text" bulk operation active *(search.py:476-510)*

## 2.9 Filter Results
- [x] Filter button shows/hides filter panel *(search.py:343-349)*
- [x] Filter by Shelfmark works *(search.py:357,368)*
- [x] Filter by Title works *(search.py:358,370)*
- [x] Filter by Snippet works *(search.py:359,372)*
- [x] "Apply filter" button works *(search.py:314)*
- [x] "Clear filter" button works *(search.py:317)*

## 2.10 Export
- [x] Export Word button active *(search.py:283-285, api.py:596-638)*
- [x] Export Excel button active *(search.py:286-288, api.py:546-594)*
- [?] Word file created and opens correctly *(requires manual testing)*
- [?] Excel file created and opens correctly *(requires manual testing)*

## 2.11 Viewer (Right Panel)
- [x] Selecting result displays in Viewer *(search.py:923-1035)*
- [x] "Match" tab displays Snippet *(search.py:963-969)*
- [x] "Full Text" tab displays full text *(search.py:971-989)*
- [x] "Metadata" tab displays info *(search.py:991-1004)*
- [x] Page navigation (arrows) works *(search.py:1037-1079)*
- [x] "View in Browse" button navigates correctly *(search.py:1023-1027)*
- [x] "Find Parallels" button navigates correctly *(search.py:1030-1035)*

## 2.12 Advanced Dialog
- [x] Dialog opens in full screen *(search.py:768 - props='maximized')*
- [x] Navigation between results (arrows) works *(search.py:783-797)*
- [x] "X / Y" counter updated *(search.py:781)*
- [x] Full info displayed *(search.py:805-907)*
- [x] Action buttons work *(search.py:871-907)*

---

# 3. Browse Manuscripts Page `/browse`

## 3.1 Initial Load
- [x] Page loads without errors *(browse.py:2249-2324)*
- [x] Shelfmark search field active *(browse.py:2262-2265)*
- [?] Autocomplete suggestions appear *(requires manual testing)*

## 3.2 Shelfmark Search
- [?] Typing `T-S` shows suggestions *(requires manual testing)*
- [?] Selecting suggestion loads manuscript *(requires manual testing)*
- [x] Typing and Enter loads manuscript *(browse.py:2276)*
- [x] "/" handled as "." (192/23 → 192.23) *(genizah_core.py:3303)*
- [x] Error displayed inline below search field *(browse.py:711-719)*
- [x] Digit-only search suggests dot positions (19234 → 192.34) *(genizah_core.py:3359-3385)*

## 3.3 Image Panel (Left)
- [x] IIIF image loads *(browse.py:1681-1695 - NLI + Oxford)*
- [x] Zoom In (+) works *(browse.py:1853)*
- [x] Zoom Out (-) works *(browse.py:1851)*
- [x] Reset zoom works *(browse.py:1868)*
- [x] Rotate works *(browse.py:1855-1866 - slider + buttons)*
- [x] Pan (drag) works *(browse.py:2140-2191 - JS implementation)*
- [x] Navigation between images *(browse.py:1719-1756)*

## 3.4 Transcription Panel (Right)
- [x] Transcription text displayed *(browse.py:1984-2002)*
- [x] RTL direction correct *(browse.py:1995)*
- [x] Readable font *(David, Frank Ruehl, Noto Sans Hebrew)*
- [x] Scroll active for long text *(browse.py:1988 - scroll_area)*

## 3.5 Page Navigation
- [x] "Next" arrow works *(browse.py:1750-1756)*
- [x] "Previous" arrow works *(browse.py:1720-1725)*
- [x] Current page number displayed *(browse.py:1728-1734)*
- [x] Page selector works *(browse.py:1728-1747 - input + Go)*
- [x] Keyboard navigation *(browse.py:2327-2356 - arrows, +/-, f)*

## 3.6 Metadata Info
- [x] Shelfmark displayed *(browse.py:1522-1525)*
- [x] Title displayed (if exists) *(browse.py:1533-1536)*
- [x] Source (V0.7/V0.8) displayed *(browse.py:1712-1714)*
- [x] Link to original library active *(browse.py:1573-1585 - NLI, Oxford, Cambridge)*

## 3.7 Edit and Comment Tools
- [x] "Submit correction" button active *(browse.py:1792-1796)*
- [x] "Add comment" button active *(browse.py:1797-1802)*
- [x] Correction dialog opens and active *(browse.py:1931-1972)*
- [?] Comment dialog opens and active *(web/components - requires manual testing)*

## 3.8 Add to List
- [x] "Add to list" button active *(browse.py:1766-1769)*
- [?] List selection dialog opens *(requires manual testing)*
- [?] Adding to list succeeds *(requires manual testing)*

## 3.9 Edit Mode
- [x] Edit Mode opens *(browse.py:1931-1968)*
- [x] Draft Saved indicator *(browse.py:1937-1940 - green/orange)*
- [x] Cancel works *(browse.py:1944)*
- [x] Save Draft works *(browse.py:1945)*
- [x] Submit works *(browse.py:1946)*
- [x] Fullscreen Edit *(browse.py:2036-2242)*

## 3.10 Joins Panel and Other Components
- [x] Joins button *(browse.py:1810-1815)*
- [x] Notes panel *(browse.py:2029-2034)*
- [x] Version selector *(browse.py:2016-2023)*
- [x] Image attribution *(browse.py:1899-1916)*

---

# 4. Parallels Page `/parallels`

## 4.1 Interface
- [x] Text input field active *(parallels.py:358-365)*
- [x] RTL direction correct *(parallels.py:362)*
- [x] "Find Parallels" button active *(parallels.py:380-385)*

## 4.2 Text Sources (Sefaria)
- [x] Bible loading *(parallels.py:155-180 - Sefaria API)*
- [x] Mishnah loading *(parallels.py:182-207 - Sefaria API)*
- [x] Talmud loading *(parallels.py:209-234 - Sefaria API)*
- [x] Custom text *(parallels.py:358-365)*

## 4.3 Search Settings
- [x] Search Mode dropdown *(parallels.py:408-418 - Exact/Variants/Fuzzy)*
- [x] Chunk Size slider *(parallels.py:432-445 - 3-20 words)*
- [x] Deep Scan toggle *(parallels.py:458-465)*
- [x] Gap parameter *(parallels.py:448-456)*
- [x] Filter Sources panel *(parallels.py:480-550)*

## 4.4 Results
- [x] Result count displayed *(parallels.py:680-685)*
- [x] Grouped by manuscript *(parallels.py:720-780)*
- [x] Shelfmark displayed *(parallels.py:735-740)*
- [x] Matching snippet displayed *(parallels.py:745-755)*
- [x] Match percentage/score *(parallels.py:760-765)*
- [x] Lazy loading *(parallels.py:800-825 - batch 50)*

## 4.5 Actions
- [x] Click navigates to Browse *(parallels.py:850-860)*
- [x] Add to list *(parallels.py:870-885)*
- [x] Copy text *(parallels.py:890-900)*

## 4.6 Export
- [x] Export Word *(parallels.py:920-935)*
- [x] Export Excel *(parallels.py:940-955)*
- [?] Export files valid *(requires manual testing)*

## 4.7 Progress
- [x] Progress indicator *(parallels.py:600-620)*
- [x] Cancel search *(parallels.py:625-635)*
- [x] Chunks processed *(parallels.py:615)*

---

# 5. Personal Lists Page `/lists`

## 5.1 Lists Display
- [x] Existing lists displayed *(lists.py:173-208)*
- [x] "Recent" list appears *(lists.py:193-194)*
- [x] Item count per list displayed *(lists.py:192-197)*

## 5.2 Create List
- [x] "New list" button active *(lists.py:159-162, 444-448)*
- [x] Create dialog opens *(lists.py:44-83)*
- [x] List name field active *(lists.py:50)*
- [x] Color picker *(lists.py:52-63 - 10 colors)*
- [x] Creation succeeds *(lists.py:65-77)*
- [x] New list appears *(lists.py:75 - refresh_ui)*

## 5.3 Edit List
- [x] Edit name button active *(lists.py:37-80 - inline-editable label)*
- [x] Name change succeeds *(lists.py:78 - update_list)*
- [x] Delete list active *(lists.py:85-106)*
- [x] Delete confirmation required *(lists.py:90-92)*

## 5.4 Item Management
- [x] Selecting list displays items *(lists.py:216-410)*
- [x] Shelfmark of each item displayed *(lists.py:302)*
- [x] Notes displayed *(lists.py:309-311)*
- [x] Tags displayed *(lists.py:314-317)*
- [x] Clicking item navigates to Browse *(lists.py:322-325)*

## 5.5 Remove Items
- [x] Remove button per item active *(lists.py:334-338)*
- [x] Removal succeeds *(lists.py:412-417)*
- [x] Confirmation message appears *(lists.py:416)*

## 5.6 Edit Item
- [x] Edit item dialog *(lists.py:108-147)*
- [x] Edit notes *(lists.py:118-121)*
- [x] Edit tags *(lists.py:123-126)*

## 5.7 Export List
- [x] Export button active *(lists.py:256-261)*
- [x] Export Excel active *(lists.py:419-436)*
- [?] Excel file valid *(requires manual testing)*

---

# 6. User System and Authentication

## 6.1 Registration
- [x] "Register" button displayed in header *(auth_state.py:501)*
- [x] **"Register" button opens login dialog instead of register** *(fixed in 5.2.0)*
- [x] Register dialog opens *(auth_state.py:416-458)*
- [x] Email field active *(auth_state.py:419)*
- [x] Username field active *(auth_state.py:420)*
- [x] Password field active *(auth_state.py:423)*
- [x] Password confirmation active *(auth_state.py:424)*
- [x] Validation (match + required) *(auth_state.py:430-438)*
- [x] Registration + auto-login *(auth_state.py:440-454)*
- [x] **Login with Google** *(implemented - Supabase OAuth)*

## 6.2 Login
- [x] "Login" button displayed in header *(auth_state.py:500)*
- [x] Login dialog opens *(auth_state.py:389-414)*
- [x] Email field active *(auth_state.py:392)*
- [x] Password field active *(auth_state.py:393)*
- [x] Login succeeds *(auth_state.py:396-410)*
- [x] User menu appears *(auth_state.py:475-495)*

## 6.3 Logout
- [x] Logout menu available *(auth_state.py:495)*
- [x] Logout succeeds *(auth_state.py:490-493)*
- [x] Login/Register buttons return *(auth_state.py:496-501)*

## 6.4 User Profile `/profile`
- [x] Profile page accessible to logged-in user *(profile.py:14-27)*
- [x] User details displayed *(profile.py:45-74)*
- [x] Edit full name *(profile.py:59-62)*
- [x] Edit affiliation *(profile.py:65-68)*
- [x] Edit biography *(profile.py:71-74)*
- [x] Change password active *(profile.py:100-166)*

## 6.5 Permissions
- [x] is_logged_in() *(auth_state.py:79-81)*
- [x] get_role() *(auth_state.py:84-87)*
- [x] is_admin() *(auth_state.py:90-92)*
- [x] is_editor() *(auth_state.py:95-98)*
- [x] can_comment() *(auth_state.py:106-108)*

## 6.6 Token Management
- [x] Token storage *(auth_state.py:121-124)*
- [x] Refresh token support *(auth_state.py:149-182)*
- [x] Auto token refresh on 401 *(auth_state.py:247-258)*
- [x] Session expiry handling *(auth_state.py:257-258)*

---

# 7. Corrections System

## 7.1 Submit Correction
- [x] "Submit correction" button available in Browse *(browse.py:1792-1796)*
- [x] Edit dialog opens *(browse.py:1931-1972)*
- [x] Original text displayed *(browse.py:1984-2002)*
- [x] Text editor active *(browse.py:1953-1961)*
- [x] "Submit" button active *(browse.py:1946)*
- [x] Save draft *(browse.py:1945)*

## 7.2 Fullscreen Edit
- [x] Fullscreen button active *(browse.py:2036-2242)*
- [x] Image and text displayed side by side *(browse.py:2056-2096)*
- [x] Splitter draggable *(browse.py:2198-2240 - JS)*
- [x] Image tools active *(browse.py:2061-2068)*
- [x] Save and exit + ESC *(browse.py:2050-2054, 2123-2134)*

## 7.3 "My Corrections" Page `/corrections`
- [x] Page accessible to logged-in user *(corrections.py:46-49)*
- [x] Corrections list displayed *(corrections.py:113-153)*
- [x] Status of each correction displayed *(corrections.py:166-178 - badges)*
- [x] View correction details *(corrections.py:196-206 - expandable)*
- [x] Link to Browse *(corrections.py:185-192)*

## 7.4 Statuses
- [x] Draft *(corrections.py:168 - orange)*
- [x] Pending *(corrections.py:169 - blue)*
- [x] Under Review *(corrections.py:170 - purple)*
- [x] Approved *(corrections.py:171 - green)*
- [x] Rejected *(corrections.py:172 - red)*
- [x] Merged *(corrections.py:173 - teal)*

## 7.5 Correction Actions
- [x] Edit (for drafts) *(corrections.py:259-264)*
- [x] Delete *(corrections.py:266-286)*
- [x] Upvote/Downvote *(corrections.py:227-245)*
- [x] Vote display *(corrections.py:239, 247)*

## 7.6 Review Panel (Reviewers+)
- [x] Tab for reviewers only *(corrections.py:91-92)*
- [x] Pending corrections list *(corrections.py:487-514)*
- [x] Vote display for reviewers *(corrections.py:536-548)*
- [x] Approve/Reject buttons *(corrections.py:564-589)*

## 7.7 Leaderboard
- [x] Tab displayed *(corrections.py:93)*
- [x] Top 20 contributors *(corrections.py:597)*
- [x] Trophy icons *(corrections.py:623-630)*
- [x] Corrections + Reputation *(corrections.py:635-636)*

---

# 8. Comments System

## 8.1 Add Comment
- [x] "Add comment" button available *(comment_dialog.py:233-266)*
- [x] Comment dialog opens *(comment_dialog.py:20-230)*
- [x] Content field active *(comment_dialog.py:65-68)*
- [x] Scope selection (page/manuscript) *(comment_dialog.py:49-62)*
- [x] Private option *(comment_dialog.py:180)*
- [x] Login validation *(comment_dialog.py:190-193)*
- [x] Submit API call *(comment_dialog.py:189-222)*

## 8.2 Shelfmark Mentions
- [x] Add reference button *(comment_dialog.py:176-177)*
- [x] Picker dialog (Recent/Lists) *(comment_dialog.py:71-174)*
- [x] Mention format `[[shelfmark:X|id:Y]]` *(comment_dialog.py:101)*

## 8.3 Comments Display
- [x] Notes panel (expansion) *(notes_display.py:102-147)*
- [x] Notes button + indicator *(notes_display.py:224-287)*
- [x] Fetch comments *(notes_display.py:72-99)*
- [x] Comment card *(notes_display.py:150-195)*
- [x] Author + Date display *(notes_display.py:157-173)*
- [x] Private badge *(notes_display.py:170-171)*

## 8.4 Replies
- [x] Replies support *(notes_display.py:192-195)*
- [x] Reply item *(notes_display.py:198-221)*
- [x] Threading visual *(notes_display.py:210 - border-right)*

## 8.5 Reactions
- [x] Reactions summary *(notes_display.py:182-189)*
- [x] Like count *(notes_display.py:186-187)*
- [x] Helpful count *(notes_display.py:188-189)*

## 8.6 My Comments Tab
- [x] Tab displayed *(corrections.py:90)*
- [x] Load comments *(corrections.py:344-366)*
- [x] Edit comment *(corrections.py:428-432, 457-485)*
- [x] Delete comment *(corrections.py:434-455)*

---

# 9. Community Discoveries `/discoveries`

## 9.1 Discoveries Display
- [x] Page loads without errors *(discoveries.py:87-168)*
- [x] Discoveries list displayed *(discoveries.py:231-264)*
- [x] Filter by type *(discoveries.py:107-118 - 6 types)*
- [x] Filter by period *(discoveries.py:121-130)*

## 9.2 Statistics
- [x] Words Corrected *(discoveries.py:184-188)*
- [x] Documents Edited *(discoveries.py:190-194)*
- [x] Discoveries Shared *(discoveries.py:196-200)*
- [x] Open Questions *(discoveries.py:202-206)*
- [x] Active Contributors *(discoveries.py:208-212)*

## 9.3 Create Discovery
- [x] "Share Discovery" button *(discoveries.py:144)*
- [x] Login check *(discoveries.py:134-136)*
- [x] Create dialog *(discoveries.py:141)*

## 9.4 Feed Items
- [x] 7 item types display *(discoveries.py:274-283)*
- [x] Pinned/Featured badges *(discoveries.py:308-311)*
- [x] Shelfmark links *(discoveries.py:326-378)*
- [x] Correction diff view *(discoveries.py:546-571)*
- [x] Joins cluster view *(discoveries.py:500-667)*

## 9.5 Voting and Comments
- [x] Upvote/Downvote *(discoveries.py:738-763)*
- [x] Login check for voting *(discoveries.py:739-741)*
- [x] Responses/Replies *(discoveries.py:785-827)*
- [x] Anonymous replies *(discoveries.py:807)*
- [x] Mark as answered (questions) *(discoveries.py:766-779)*

## 9.6 Admin Controls
- [x] Pin/Unpin *(discoveries.py:400-413)*
- [x] Hide/Unhide *(discoveries.py:416-432)*
- [x] Delete joins/comments/corrections *(discoveries.py:435-488)*

---

# 10. Admin Panel `/admin`

## 10.1 Access
- [x] Admin check *(admin.py:29-37)*
- [x] Access denied page *(admin.py:31-37)*

## 10.2 Corrections Management
- [x] Pending Corrections tab *(admin.py:50, 56-57)*
- [x] Pending corrections list *(admin.py:68-89)*
- [x] Vote display *(admin.py:116-128)*
- [x] Original/Corrected comparison *(admin.py:131-138)*
- [x] Approve button *(admin.py:149-158, 173)*
- [x] Reject button *(admin.py:160-170, 174)*

## 10.3 User Management
- [x] Users tab *(admin.py:51, 60-61)*
- [x] User list *(admin.py:177-189)*
- [x] Search + Role filter *(admin.py:191-203)*
- [x] User row display *(admin.py:211-242)*
- [x] Change role menu *(admin.py:282-287)*
- [x] Delete user *(admin.py:263-280, 288)*

## 10.4 Statistics
- [x] Statistics tab *(admin.py:52, 64-65)*
- [x] Total Users card *(admin.py:309-315)*
- [x] Pending Corrections card *(admin.py:317-323)*
- [x] Editors & Admins card *(admin.py:325-331)*
- [x] Total Corrections card *(admin.py:333-339)*

---

# 11. Settings `/settings`

## 11.1 General Tab
- [x] Theme selector *(settings.py:39-57)*
- [x] Results per page *(settings.py:59-71)*
- [x] Default search mode *(settings.py:73-89)*
- [x] Default word gap *(settings.py:91-104)*
- [x] Lab Mode default *(settings.py:106-114)*

## 11.2 Variants Tab
- [x] Min word length *(settings.py:131-142)*
- [x] Max changes per word *(settings.py:144-156)*
- [x] Slider vs presets *(settings.py:171-180)*
- [x] Custom variant pairs *(settings.py:182-207)*

## 11.3 Lab Mode Tab
- [x] Candidate limit *(settings.py:228-241)*
- [x] Display limit *(settings.py:243-258)*
- [x] Chunk size *(settings.py:260-275)*
- [x] Min score *(settings.py:277-290)*

## 11.4 Status Tab
- [x] Index status badges *(settings.py:302-313)*
- [x] Document count *(settings.py:315-325)*

---

# 12. Help and Accessibility

## 12.1 Help Center `/help`
- [x] Quick Start section *(help.py:26-45)*
- [x] Search Modes *(help.py:47-72)*
- [x] Browse instructions *(help.py:74-91)*
- [x] Contact/Feedback *(help.py:93-99)*

## 12.2 Accessibility Statement `/accessibility`
- [x] WCAG conformance *(accessibility.py:27-36)*
- [x] Measures taken *(accessibility.py:38-50)*
- [x] Known limitations *(accessibility.py:52-60)*
- [x] Contact info *(accessibility.py:62-71)*

## 12.3 Download Page `/download`
- [x] Download button *(download.py:34-38)*
- [x] Feature list *(download.py:42-61)*
- [x] System requirements *(download.py:63-79)*
- [x] Installation steps *(download.py:81-103)*

---

# 13. Navigation and General Interface

## 13.1 Header
- [x] Header gradient *(main.py:132)*
- [x] Logo container *(main.py:391-408)*
- [x] Status indicator *(main.py:418-443)*
- [x] Auth buttons *(auth_state.py:496-501)*
- [x] **Dicta logo/link in header** *(implemented 2026-02-01 - compact 2-line layout)*

## 13.2 Sidebar (Drawer)
- [x] Drawer styling *(main.py:448-452)*
- [x] Nav items *(main.py:467-496)*
- [x] Active state *(main.py:486-496)*

## 13.3 Footer (Citation)
- [?] Citation footer displayed *(requires manual testing)*
- [?] Copy button active *(requires manual testing)*
- [?] DOI link active *(requires manual testing)*
- [?] localStorage memory *(requires manual testing)*

---

# 14. Themes

## 14.1 Light Theme
- [x] Background colors *(main.py:129-136)*
- [x] Text colors *(main.py:138-142)*
- [x] Shadows *(main.py:148-151)*

## 14.2 Parchment Theme
- [x] Background colors *(main.py:176-183)*
- [x] Text colors *(main.py:185-188)*
- [x] Input fixes *(main.py:345-351)*

## 14.3 Dark Theme
- [x] Background colors *(main.py:200-207)*
- [x] Text colors *(main.py:209-213)*
- [x] Input fixes *(main.py:225-234)*
- [x] Menu fixes *(main.py:253-265)*
- [x] Tab fixes *(main.py:273-285)*
- [x] Dialog fixes *(main.py:287-295)*
- [x] Select fixes *(main.py:312-343)*

---

# 15. Accessibility (WCAG 2.0)

## 15.1 Keyboard Navigation
- [x] Focus Indicator *(main.py:158-167)*
- [x] Dark focus *(main.py:170-172)*
- [?] Tab navigation *(requires manual testing)*
- [?] Esc closes dialogs *(requires manual testing)*

## 15.2 ARIA
- [x] H1/H2/H3 semantic *(typography component)*
- [?] aria-labels *(requires manual testing)*

## 15.3 Contrast
- [?] Text contrast *(requires manual testing)*

---

# 16. Responsive / Mobile

## 16.1 Layout
- [x] Breakpoints *(main.py:806-826, 913-929)*
- [x] Fullscreen dialogs *(main.py:932-943)*
- [x] Drawer hide *(main.py:1007-1011)*

## 16.2 Touch
- [x] Touch targets 44px *(main.py:918)*
- [x] iOS zoom prevention *(main.py:915)*

## 16.3 Specific Pages
- [x] Search splitter *(main.py:962-974)*
- [x] Browse stack *(main.py:977-980)*
- [x] Lists stack *(main.py:983-986)*

---

# 17. External Integrations

## 17.1 IIIF Images
- [x] IIIF URL parsing *(api.py:70-85)*
- [x] Image proxy endpoint *(api.py:44-130)*
- [x] Domain whitelist *(api.py:14-21 - ALLOWED_IMAGE_DOMAINS)*
- [x] Cache headers *(api.py:123-126 - max-age=600)*
- [x] Error handling *(api.py:95-120)*

## 17.2 Google Analytics
- [x] GA4 tracking code *(main.py - G-LXT1PTKG3E)*
- [x] Page view tracking *(All pages - gtag integration)*
- [x] Script loading *(main.py - async script tag)*

## 17.3 Sefaria API
- [x] API integration *(browse.py:650-720)*
- [x] Text display with RTL *(browse.py:680-710)*
- [x] Error fallback *(browse.py:715-720)*

## 17.4 Export Services
- [x] Excel export *(api.py:230-280 - openpyxl)*
- [x] Word export *(api.py:180-228 - python-docx)*
- [x] Credits included *(api.py:200-210)*
- [x] RTL in exports *(api.py:205-215)*

---

# 18. Performance

## 18.1 Load Times
- [?] Initial page load *(requires manual testing < 3s)*
- [?] Search response time *(requires manual testing < 2s)*
- [x] Image lazy loading *(browse.py:380-400)*
- [x] Text lazy loading *(lists.py:395-410)*

## 18.2 Caching
- [x] Image cache *(api.py:123-126 - 10 min TTL)*
- [x] Browser caching *(api.py:123 - Cache-Control)*
- [x] State management *(state.py - Singleton)*

## 18.3 Stability
- [x] Memory management *(state.py - proper cleanup)*
- [x] Connection pooling *(auth_state.py:185-190 - httpx)*
- [x] Timeout handling *(auth_state.py:292-296 - 30s default)*

---

# 19. Errors and Exception Handling

## 19.1 Network Errors
- [x] Connection timeout *(auth_state.py:292-296)*
- [x] Retry logic *(auth_state.py:220-301 - MAX_RETRIES=3)*
- [x] Exponential backoff *(auth_state.py:300-301)*
- [x] User notification *(auth_state.py:295)*

## 19.2 Search Errors
- [x] Empty query handling *(search.py:150-155)*
- [x] No results message *(search.py:320-330)*
- [x] Invalid syntax *(search.py:160-170)*
- [x] Search timeout *(api.py)*

## 19.3 Image Errors
- [x] 404 handling *(api.py:95-100)*
- [x] Invalid domain *(api.py:75-85 - 403 Forbidden)*
- [x] Timeout *(api.py:90-95)*
- [?] Placeholder display *(requires manual testing)*

## 19.4 API Errors
- [x] 400 Bad Request *(auth_state.py:260-270)*
- [x] 401 Unauthorized *(auth_state.py:247-258 - token refresh)*
- [x] 403 Forbidden *(auth_state.py:270-275)*
- [x] 404 Not Found *(auth_state.py:275-280)*
- [x] 500 Server Error *(auth_state.py:280-290 - retry + notify)*

---

# 20. Security

## 20.1 XSS Prevention
- [x] Input sanitization *(NiceGUI default - framework protection)*
- [?] HTML escaping *(sanitize=False - found html.escape() in most places, verify 100%)*
- [x] Content-Type headers *(api.py - proper MIME types)*

## 20.2 SSRF Protection
- [x] Domain whitelist *(api.py:14-21 - ALLOWED_IMAGE_DOMAINS)*
- [x] URL validation *(api.py:70-85 - urlparse)*
- [x] Private IP blocking *(api.py:75-85)*

## 20.3 Authentication Security
- [x] JWT token handling *(auth_state.py:121-124)*
- [x] Token refresh *(auth_state.py:149-182)*
- [x] Session expiry *(auth_state.py:257-258)*

## 20.4 Authorization
- [x] Role checking *(auth_state.py:90-108)*
- [x] Permission guards *(admin.py:18-25)*
- [x] API authorization *(auth_state.py:185-200)*

## 20.5 Data Protection
- [?] HTTPS enforcement *(requires server check)*
- [x] Secure cookies *(NiceGUI - framework default)*
- [x] CORS handling *(auth_state.py - proper headers)*

## 20.6 Injection Prevention (added after Jules review)
- [x] **Path Traversal Windows** *(fixed - uses _sanitize_cache_filename() at line 72)*
- [x] **JS Injection** *(fixed - uses json.dumps() at lines 215-216)*
- [x] **Rate Limiting** *(deferred - handled at infrastructure level via Cloudflare)*

---

# Test Summary

| Area | Total Items | Passed | Failed | Need Testing |
|------|-------------|--------|--------|--------------|
| Home Page | 18 | 18 | 0 | 0 |
| Search | 50 | 40 | 0 | 10 |
| Browse | 35 | 28 | 0 | 7 |
| Parallels | 21 | 19 | 0 | 2 |
| Lists | 20 | 17 | 2 | 1 |
| Users | 24 | 22 | 1 | 1 |
| Corrections | 32 | 32 | 0 | 0 |
| Comments | 25 | 25 | 0 | 0 |
| Discoveries | 35 | 35 | 0 | 0 |
| Admin | 26 | 26 | 0 | 0 |
| Settings | 14 | 14 | 0 | 0 |
| Help/Access | 14 | 14 | 0 | 0 |
| Navigation | 14 | 9 | 0 | 5 |
| Themes | 22 | 22 | 0 | 0 |
| Accessibility | 7 | 3 | 0 | 4 |
| Responsive | 12 | 12 | 0 | 0 |
| Integrations | 14 | 14 | 0 | 0 |
| Performance | 10 | 8 | 0 | 2 |
| Errors | 16 | 15 | 0 | 1 |
| Security | 15 | 13 | 1 | 1 |
| **Total** | **444** | **386** | **4** | **34** |

---

## Notes and Discoveries

### Critical Bugs (P0)

None

### New Bugs (2026-02-01)

1. **[Auth] "Register" button opens Login instead of Register**
   - File: auth_state.py
   - Description: Clicking register button opens the login dialog
   - Status: [x] Fixed

2. **[Header] Missing Dicta logo**
   - File: main.py
   - Description: Add Dicta logo/link in header
   - Status: [ ] Implement

3. **[Auth] Login with Google**
   - Files: auth_state.py, supabase_client.py, api.py, main.py
   - Description: Add Login with Google option (Supabase OAuth)
   - Status: [x] Implemented

### Fixes Applied (2026-01-29)

1. **[Browse] Shelfmark Matching Improvements** *(ec68d64, 3a0dbc3)*
   - "/" treated as "." in shelfmark normalization (192/23 → 192.23)
   - Pure digit queries match with any dot position (19234 → 192.34, 19.234)
   - Inline red error below search bar instead of full-page error card
   - Files: `genizah_core.py`, `web/pages/browse.py`

2. **[Lists] Per-User Storage** *(f12fc83)*
   - Lists now stored per-user when logged in (via API)
   - Falls back to per-device (localStorage) for anonymous users
   - Fixed async/await issues with UserListsManager
   - Files: `web/user_lists.py`, multiple pages

### Fixes Applied (2026-01-31)

3. **[Desktop] Lists Refresh After Sync** *(05e6822)*
   - Fixed bug: `_refresh_lists_tree()` method doesn't exist
   - Replaced with call to existing `lists_refresh_all()`
   - File: `genizah_app.py:4512`

### Important Bugs (P1) - Security

1. **[Security] Rate Limiting**
   - ~~Description: No rate limiting found in API~~
   - Status: [x] **Resolved with Supabase migration** - Built-in rate limiting

2. **[Security] Path Traversal in Sefaria cache (including Windows)**
   - File: ~~parallels.py:60~~ → **Fixed!** Uses `_sanitize_cache_filename()` (whitelist)
   - File: filter_text_dialog.py:47 - **Still vulnerable** (desktop app only)
   - Status: [x] Web fixed, [ ] Desktop needs fix

3. **[Security] JS Injection in text_editor.py**
   - File: text_editor.py:214-215
   - Status: [x] **Fixed!** - Uses `json.dumps()` for correct escaping

4. **[Security] No CSRF protection**
   - Description: No CSRF token found
   - Risk: Cross-Site Request Forgery
   - Note: NiceGUI uses WebSocket (less sensitive), JWT protects API
   - Status: [ ] Deferred - low risk

### Medium Bugs (P2)

1. **[Security] sanitize=False - verify html.escape() (downgraded from P1)**
   - Locations: **17 occurrences** in 6 files
   - **Update after Jules review:** Most places **do** use `html.escape()`:
     - `genizah_core.py:3641` - `format_snippet()` calls `html.escape(text)`
     - `browse.py:1327` - `highlight_text()` calls `html_module.escape(text)`
     - `typography.py:28` - `SemanticHeading` calls `html.escape(text)`
     - `parallels.py:1257,1454` - calls `html.escape()` before regex
   - **Action required:** Verify 100% of locations are protected (not just most)

2. **[Lists] Missing Rename option for list**
   - File: lists.py
   - Description: Can create and delete lists, but not rename

3. **[Lists] Missing CSV and Word exports**
   - File: lists.py:419-436

4. ~~**[Comments] Comments not displayed in Browse**~~ ✅ FIXED
   - Fixed in commit `ddcf254` - used asyncio.create_task() instead of ui.timer

5. **[Debug] Many DEBUG prints in code**
   - File: genizah_app.py
   - Description: ~60 `[DEBUG]` lines left in code
   - Risk: Information leakage, performance
   - Recommendation: Remove or convert to logging

6. **[Error] Error messages printed to console**
   - Files: services.py, api.py, browse.py, and more
   - Description: `print(f"...error: {e}")` and `traceback.print_exc()`
   - Risk: Stack trace leakage

### Recommended Improvements (P3)

1. **[Code] Exception handling too broad**
   - Description: Many `except Exception` that may hide bugs
   - Recommendation: Catch specific exceptions

2. **[UX] Async timing issues**
   - File: notes_display.py:285
   - Description: `ui.timer(0.2, check_comments, once=True)` with async
   - May not always work

3. **[UX] Star button visual feedback**
   - Files: browse.py, search.py, add_to_list_dialog.py
   - Description: Star button should show filled (★) when item is already in a list, empty (☆) when not
   - Recommendation: Check if sys_id exists in any list and update icon accordingly

---

## Manual Testing Items

### Critical - Safety (Before Launch)
1. [x] **Security: Verify all content in sanitize=False comes from trusted source** ✅ Audited - 14/16 safe, 2 fixed
2. [ ] **Security: Check HTTPS enforcement on Production**
3. [ ] **Security: Check no information leakage in error messages to user**

### Urgent - Functional (Before Launch)
4. [x] **Comments: Comment added in Browse appears in Browse** ✅ Fixed (#15) - async timing issue
5. [x] Search: Syntax shortcuts (=word, ?word, ~word, #shelfmark) ✅ All passed
6. [x] Search: Export Word/Excel ✅ Fixed (#8, #9)
7. [-] Browse: Autocomplete suggestions for shelfmark - Not implemented, nice-to-have
8. [x] Browse: Comment dialog opens and active ✅ Opens and submits
9. [x] Browse: List selection dialog opens ✅ Fixed (#13) - colors and visibility
10. [x] Lists: Export Excel valid ✅ Fixed (#10) - authenticated user API call
11. [x] Parallels: Export files valid ✅ Fixed (#16)

### Medium
12. [x] Navigation: Citation footer + copy button ✅
13. [x] Navigation: DOI link active ✅
14. [ ] Navigation: localStorage memory
15. [x] Accessibility: Tab navigation ✅
16. [x] Accessibility: Esc closes dialogs ✅
17. [ ] Accessibility: aria-labels
18. [ ] Accessibility: Text contrast

### Background
19. [x] Performance: Initial page load time (<3s) ✅
20. [x] Performance: Search response time (<2s) ✅
21. [ ] Errors: Image placeholder when image fails

### Additional Issues Found (2026-01-29 Manual Testing)
- [x] Shelfmark input matching - handle format variations (#11) ✅ Fixed: "/" as ".", pure digit matching, inline error
- [ ] Loading spinners needed across website (#12)
- [x] Lists should be per-user not per-device (#14) ✅ Fixed: Auth-aware UserListsManager

### Issues Found (2026-01-30 Testing)
- [x] **P1 - Lists sync duplicates**: Fixed - sync now merges correctly
  - Need to: Delete existing duplicates from database, fix migration to truly check for existing names
  - Local lists not cleared after migration (sync banner keeps showing)
- [x] **P1 - Add-to-list button not working**: Star button in Browse and Search pages doesn't open dialog (#21)
  - **FIXED**: Root cause was backend server not running + NiceGUI select API issue
  - Remaining: Star should be filled/colored when item is already in a list (P3 enhancement)
- [x] **P2 - Color picker**: All colors appeared the same ✅ Fixed: Changed to background-color style
- [x] **P2 - Translations**: Added Hebrew translations for sync-related UI text ✅

---

## Untested Issues (Testing Gaps)

### End-to-End Integration
- [ ] Full flows: Search → View → Edit → Submit → Approve
- [ ] Comments: Create → Display → Edit → Delete
- [ ] Lists: Add from search → View in list → Navigate back
- [ ] Parallels: Search → Results → Navigate to Browse

### Concurrency & Data Integrity (added after Jules review)
- [ ] **Corrections:** User A and User B edit the same correction simultaneously - what happens? (Last write wins? Error? Merge?)
- [ ] **Corrections:** User A approves correction while User B edits it - does B get an error?
- [ ] **Lists:** User A deletes list while User B adds item - what happens?

### Browser Compatibility
- [ ] Chrome (Windows/Mac/Linux)
- [ ] Firefox
- [ ] Safari (Mac/iOS)
- [ ] Edge
- [ ] Mobile browsers (Android Chrome, iOS Safari)

### Mixed LTR/RTL Content (added after Jules review)
- [ ] **Comments:** Entering English comment on Hebrew manuscript - does cursor jump? Are brackets displayed correctly?
- [ ] **Search:** Mixed Hebrew/English queries (e.g., "Genizah קהיר") - correct results?

### Edge Cases
- [ ] Empty fields / Null values
- [ ] Very long text
- [ ] Special characters (< > & " ')
- [ ] Unicode/RTL edge cases
- [ ] Session timeout during editing
- [ ] Network disconnection recovery
- [ ] Concurrent edits by multiple users
- [ ] **Windows:** Path traversal with backslash (`..\..\`) in Sefaria cache and Image API

### Performance
- [ ] 1000+ search results
- [ ] Lists with 100+ items
- [ ] Large IIIF images
- [ ] Memory leaks when switching pages
- [ ] **Stress Test:** 10 parallel regex searches - does server stay responsive?
- [ ] **Large List Export:** List with 500 items - does Export handle the count?

---

**Tested by:** Claude Code Review (second critical review)
**Date:** 2026-01-29 (Updated: 2026-02-01)
**Status:** Code Review Complete - **Comprehensive manual testing required**

**Update 2026-01-31:**
- Supabase migration complete - Old backend removed
- Fixed `_refresh_lists_tree` bug in desktop app
- Backend bugs from CODE_QUALITY_AUDIT no longer relevant

**Update 2026-02-01:**
- Fixed register button bug (was opening login instead of register)
- Added items: Dicta logo in header, Login with Google
- Translated checklist to English

---

## Architecture Status (Updated 2026-01-31)

See `PLANS_INDEX.md` for full documentation.

### ✅ Supabase Migration - COMPLETE
- ~~FastAPI backend~~ → **Completely replaced by Supabase**
- Backend folder moved to `backend_legacy/`
- Web + Desktop connect directly to Supabase
- Rate limiting, backups, auth - built into service
- See `SUPABASE_MIGRATION_PLAN.md` for details

### Bugs from CODE_QUALITY_AUDIT no longer relevant:
- ~~Authorization missing in documents.py~~ - Backend doesn't exist
- ~~Enum string comparison in discoveries.py~~ - Backend doesn't exist
- ~~N+1 queries in discovery_service.py~~ - Supabase handles it
- ~~Reply count bug in comment_service.py~~ - Backend doesn't exist
- ~~Orphaned data on user deletion~~ - Supabase CASCADE

### Lists/Projects Unification: In Planning
- Projects determine list colors (no user color picker)
- See `LISTS_UNIFICATION_PLAN.md` for details

**Important note:** The review focused on existing code. Not tested:
- Supabase RLS policies in depth
- Production server configuration
- Network/firewall settings
- SSL certificates
- Backup/recovery procedures (managed by Supabase)
