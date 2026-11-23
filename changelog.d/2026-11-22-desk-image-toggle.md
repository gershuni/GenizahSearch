## [Unreleased] -- Desktop Reading Desk: the show/hide image button hides the desk's images (desktop, branch `claude/eloquent-torvalds-7802b5`)

### Fixed
- **With the Reading Desk open, the image button (🖼) hides and shows the desk's images.** It used
  to leave them and show Browse's own image viewer beside them instead, so the window had two
  image panes, the second with "No image". It now works in each of the desk's views (One at a
  time, All images, Puzzle images), Browse's viewer stays hidden while the desk is open, and a
  redraw of the desk (an image list arriving) keeps hidden images hidden.
- **The desk opens with its images on show and gives Browse its image back.** Leaving the desk
  shows or hides Browse's image as it was before the desk; a manuscript opened from the results
  while the desk is open no longer brings Browse's viewer up beside the desk's images.
- The button also works on a desk opened with nothing browsed, and while a fragment being added
  to the desk is still loading.
