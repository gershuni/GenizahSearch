## [Unreleased] -- Web: the page count of a manuscript without automatic text (web, branch `claude/awesome-lamport-1ph5sq`)

### Fixes
- **Browse and Quick View showed "Page 1 / 0" for a manuscript without any automatic transcription, and could not
  page on.** The index holds no pages of such a manuscript, so its page came with no count; Browse's only fallback
  read an `fl_id` the NLI crossref folios never carry (Cambridge images aside), and Quick View had none. Next was
  disabled, so only the first image could be seen (T-S NS 243.145 has 4). Both now count its images
  (`web/services.py::manuscript_image_count`): the NLI crossref folio list, else the NLI image list the image
  route already pages through (`web/api.py::nli_fl_ids`, the same 30-day cache; the manifest is fetched only for a
  manuscript the crossref does not know). Browse keeps the count across Next/Previous while its enrichment reruns;
  a count that cannot be known is left out rather than shown as 0.
