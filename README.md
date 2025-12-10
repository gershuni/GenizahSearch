# 📜 Genizah Search Pro 3.0

**The Ultimate Search & Analysis Tool for the Cairo Genizah Corpus**

Genizah Search Pro is a powerful desktop application designed for researchers working with Cairo Genizah manuscripts, especially with the transcriptions made available by the MiDRASH Project. **Version 3.0** introduces a complete overhaul of the user experience, focusing on speed, metadata accessibility, and reading continuity.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey) ![License](https://img.shields.io/badge/License-MIT-green)

> **⚠️ IMPORTANT: FIRST RUN**  
> After installing or upgrading to version 3.0, you must go to the **Settings & About** tab and click **"Build / Rebuild Index"**.  
> This is required to support the new line-by-line display and metadata search features.

---

## 🚀 What's New in Version 3.0?

*   **⚡ Instant Offline Metadata:** The software now includes an internal database (`libraries.csv`) with over 216,000 records, displaying Shelfmarks and Titles **instantly** alongside search results.
*   **🔎 Metadata Search:** You can now search directly for a **Shelfmark** (e.g., "T-S NS 306.15") or a **Composition Title** directly from the main search bar.
*   **📐 Original Line Breaks:** Manuscript text is now displayed line-by-line, exactly as it appears in the original transcription, rather than as a continuous block of text.
*   **📊 Sortable Results:** Click on table headers to sort search results by Shelfmark, Title, or Relevance.
*   **🖼️ Visual Preview:** Manuscript images are now displayed directly in the Search Results and Browse tabs (with smart caching for instant loading).
*   **📜 Continuous Manuscript View:** Read a manuscript naturally. The new **"View All"** feature loads all pages of a manuscript into a single, continuous scrolling view.
*   **💾 Save Entire Manuscript:** Export the full text of a multi-page manuscript into a single text file for offline study.

---

## ✨ Key Features

### 🔍 Advanced Search Engine
*   **Ultra-fast Search:** Powered by [Tantivy](https://github.com/quickwit-oss/tantivy) (Rust-based) for sub-second results.
*   **Search Modes:**
    *   **Text Search:** Exact, Fuzzy (Levenshtein), Regex, and **Variants** (Hebrew-optimized OCR correction).
    *   **Metadata Search:** Dedicated modes for **Title** and **Shelfmark** lookup.
*   **Rich Result Table:** Sortable columns, instant metadata, text snippets, and image previews.

### 🧩 Composition Analysis (Source Matching)
*   **Find Parallels:** Paste a source text (e.g., a known poem or Halakhic text) to find all its occurrences in the Genizah.
*   **Smart Grouping:** Automatically groups results by manuscript title (e.g., "Mishneh Torah") vs. "Appendix" (less certain matches).
*   **Advanced Filtering:** Filter results by specific words before or after the analysis.
*   **Split-Screen View:** Compare the Genizah fragment against your source text side-by-side with synchronized red highlighting.

### 📚 Manuscript Browser
*   **Unified Search:** Enter a System ID (`99...`) or a File ID (`FL...`) to jump directly to a specific page.
*   **Smart Image Engine:** Automatically retrieves the best available image (using NLI's IIIF/Rosetta servers) and caches it locally.
*   **Deep Links:** One-click access to the Ktiv catalog and high-res viewer.

---

## 🛠 Installation

1.  **Download:** Get the latest `GenizahSearchPro_v3.0.zip` from the releases page.
2.  **Extract:** Unzip to the folder where the Transcriptions.txt file exists (see below).
3.  **Required Data Files:**
    Ensure the following files are inside the folder next to `GenizahPro.exe`:
    *   `Transcriptions.txt` (The MiDRASH dataset, https://doi.org/10.5281/zenodo.17734473).
    *   `libraries.csv` (The metadata mapping file - **New in v3.0**).
4.  **Run:** Double-click `GenizahSearchPro.exe`.
5.	**On the first run, build the index. It will take a few minutes.

---

## 📖 Usage Guide

### 1. Standard Search
*   **Query:** Type words, shelfmarks, or titles.
*   **Mode:**
	*	**Exact:** Search for words as they are. You can set **Gap** between words.
    *   **Variants:** Best for general text search (handles OCR errors).
	*	**Fuzzy:** Other method to overcome OCR errors.
	*	**Regex:** Search with sophisticated Regular Expressions.
    *   **Shelfmark / Title:** Specific metadata lookup.
*   **Sort:** Click the "Shelfmark" or "Title" headers to organize results lexicographically.
*   **View:** Double-click a result to open the full Viewer, showing the manuscript image, text, and metadata.

### 2. Composition Search (Finding Parallels)
This tool breaks your source text into small "chunks" and searches for them in the corpus.
1.  **Input:** Paste your source text into the large text box, or click **Load Text File**.
2.  **Settings:**
    *   **Chunk:** Number of words per search block (Recommended: 4-7).
    *   **Max Freq:** Ignore common phrases that appear more than X times (reduces noise).
    *   **Filter >:** Move titles that appear many times to the "Appendix" group.
3.	**Exclude Manuscripts (Optional):** Enter known system numbers or shelfmarks of manuscripts that you want to filter.
3.  **Filter Text (Optional):** Click **Filter Text** to sort out known texts such as Bible or Mishna and Talmud.
4.  **Analyze:** Click the button to start. Results will appear in a tree structure.
5.  **Export:** Click **Save Report** to generate a detailed text file with all matches.

### 3. Browsing & Reading
*   Go to the **Browse Manuscript** tab.
*   Enter a **System ID** to load the manuscript cover, or an **FL ID** to jump to a specific image.
*   **View All:** Loads the entire manuscript text (all pages) into one scrollable window.
*   **Save:** Downloads the full text of the manuscript to your computer.

---

## 🎓 Credits & Acknowledgments

**Developed by:** Hillel Gershuni.

**Data Source:**
This tool relies on the **MiDRASH** project dataset:
> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17734473

**Libraries Used:**
*   PyQt6 (GUI Framework)
*   Tantivy (High-performance Search)
*   Requests & Urllib3 (Networking)

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.
