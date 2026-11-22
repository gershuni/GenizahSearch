## [Unreleased] -- Desktop Browse: the next and previous manuscript appear at once (desktop, branch `claude/desktop-browse-speed`)

Measured in the desktop window: moving to the next manuscript on a Cambridge shelf took 4.4-5.2 s
to its sharp image, and a return to one just seen 0.8 s. After this change the sharp image is
drawn in 6-35 ms, once and in one place, also walking on after half a second on each manuscript
(was 3.5-6 s).

### Changed
- **The manuscripts beside the one on screen are prepared ahead.** While the reader looks at a
  manuscript, Browse prepares the next two the reader's way and the one behind on the shelf
  (else in the file order): their metadata, then their first page's preview and sharp image
  into the image cache, one manuscript at a time. A move drops the work no longer needed, and
  keeps a download for a manuscript still beside the new one (`desktop/browse_prefetch.py`).
  Not inside an Oxford Part or the Reading Desk.
- **A manuscript seen in this session opens without refreshing its metadata.** Browse refreshed
  it on every visit (Cambridge's image list was downloaded again each time, nothing kept it);
  a whole answer from this session is now shown at once, as the Manuscript Viewer already did
  (`MetadataManager.enriched_recently`), and a library's image list is kept in memory
  (`fetch_external_iiif_data`, 6 hours).
- **Every library's page has a preview.** Only NLI's pages had a 400-px image first; Cambridge,
  Manchester, JTS and Penn pages now do too, through the image cache.
- **The page's image starts at once, and is painted at once.** It waited out the viewer's short
  debounce behind a quarter of a second of panel building; Browse now starts and paints it
  first (`flush_pending_page`), and keeps the previous image on screen until then, not a blank.
- **Images seen recently are kept decoded in memory** (about ten pages), so a return to one is
  drawn at once, and library connections are reused instead of a new one per image.

### Fixed
- **Cambridge: a new manuscript's first image was chosen for the wrong page.** Browse passed its
  page number (from 1) to the folio and side resolver, which counts from 0: page 1 resolved to
  the second leaf side, and the page text replaced it a moment later with a second download.
- **The image no longer jumps on arrival.** The page asked for again was started over (hidden,
  "Loading...", downloaded again); a fitted image was not centred the same way twice (the view
  scrolled over everything it had ever shown); and the top line changed height twice (shelfmark
  and title on one line, then on two).
- **GS-66: a preview and its page shared one cache file**, so the page could show blurred. NLI
  images are now kept by FL id and width; an old preview-sized page file is downloaded again.
- **A page being prepared or preloaded is not downloaded twice**: a request waits for the
  download of the same image already running. A completed download is kept even when the reader
  moved on, and cache files are written atomically (GS-76 d).
- A late preview no longer replaces a sharp image already on screen.
- Browse no longer raises "wrapped C/C++ object ... has been deleted" on a page turn or when the
  window closes (a thumbnail loader kept after it was deleted).
