# 📜 Genizah Search Pro 2.0

**Advanced Search & Analysis Tool for the Cairo Genizah Corpus**

Genizah Search Pro is a powerful desktop application designed for researchers and scholars working with Cairo Genizah manuscripts. It provides high-speed search capabilities, intelligent composition analysis, and direct integration with the National Library of Israel (NLI) Ktiv project.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey) ![License](https://img.shields.io/badge/License-MIT-green)

---

## ✨ Key Features

### 🔍 Advanced Search Engine
*   **Ultra-fast Search:** Built on the [Tantivy](https://github.com/quickwit-oss/tantivy) engine (Rust-based) for instant results across millions of fragments.
*   **Hebrew-Optimized Modes:**
    *   **Exact:** Precise sequence matching.
    *   **Variants (?):** Basic interchanges.
    *   **Extended (??):** More variants.
    *   **Maximum (???):** Aggressive swapping.
    *   **Fuzzy (~):** Levenshtein distance tolerance for typos.
    *   **Regex:** Full regular expression support.
*   **Gap Control:** Find words with specific distances between them (e.g., "Abraham ... Isaac").
*   **AI Assistant:** Integrated Google Gemini AI to help construct complex Regex queries via natural language.

### 🧩 Composition Analysis
*   **Source Matching:** Paste any text to find parallel fragments in the Genizah. It breaks the text to groups of words (default: 5) and search for them.
*   **Smart Grouping:** Automatically identifies and groups recurring compositions based on NLI information.
*   **Export Results:** Saves the results to file.

### 📚 Metadata & Browsing
*   **Live Metadata:** Fetches Shelfmarks and Titles directly from the NLI API.
*   **Manuscript Browser:** Navigate through manuscript pages (Previous/Next) directly within the app.
*   **Deep Linking:** One-click access to the **Ktiv Catalog** record and the **Ktiv Image Viewer** (direct to specific image).

---

## 🚀 Installation

1.  **Download:** Go to the [Releases](../../releases) page and download `GenizahSearchPro_v2.0.zip`.
2.  **Extract:** Unzip the folder to a location on your computer (e.g., `C:\GenizahSearch`).
3.  **Get Data:**
    *   Download the transcription dataset (`Transcriptions.txt`) from Zenodo:
    *   [**MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments**](https://doi.org/10.5281/zenodo.17734473)
4.  **Setup:** Place the `Transcriptions.txt` file **inside the same folder** as the executable (`GenizahPro.exe`).
5.  **Run:** Double-click `GenizahPro.exe`. On the first run, you will be prompted to build the index (this takes a few minutes).

---

## 🛠 Usage Guide

### Standard Search
1.  Enter terms in the **Query** box.
2.  Select a **Mode** (Variants recommended for OCR text).
3.  Set **Gap** if words are not adjacent (0 = exact phrase).
4.  Click **Search**. Double-click any result to view the full text and metadata.

### Composition Search
1.  Go to the **Composition Search** tab.
2.  **Paste** a text segment or load a text file.
3.  Adjust **Chunk Size** (words per search block) and **Max Freq** (filter out common phrases).
4.  Click **Analyze**. The results will appear as a tree structure, grouping identified manuscripts by title.

### AI Assistant
*   Click the **🤖 AI Assistant** button in the Search tab.
*   **Requires API Key:** Go to *Settings & About* > *AI Configuration* and paste your Google Gemini API Key.
*   Describe what you are looking for (e.g., *"Find words starting with Aleph and ending with Mem, 4 letters long"*).

---

## 🎓 Credits & Acknowledgments

**Developed by:** Hillel Gershuni (assisted by Gemini AI).

**Data Source:**
The software relies on the incredible work of the MiDRASH project:
> Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17734473

**Libraries Used:**
*   [PyQt6](https://www.riverbankcomputing.com/software/pyqt/) - GUI Framework
*   [Tantivy](https://github.com/quickwit-oss/tantivy) - Search Engine
*   [Google Generative AI](https://ai.google.dev/) - AI Logic

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.