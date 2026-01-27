# Antivirus False Positive Information

## Overview

GenizahSearchPro may be flagged by some antivirus software (particularly Avast, AVG, and Windows Defender) as potentially malicious. **These are false positives.** The application is completely safe and open source.

### Why Does This Happen?

The false positives occur because:

1. **PyInstaller Bundling**: The application is built using PyInstaller, which bundles Python code into an executable. The way PyInstaller extracts and runs code at runtime can trigger heuristic-based antivirus detections.

2. **Native Rust Libraries**: The Tantivy search engine library is written in Rust and compiled to native code. Some antivirus scanners flag unfamiliar native binaries.

3. **Windows API Calls**: The application uses legitimate Windows API calls (via ctypes) for system integration, which can be flagged by aggressive heuristics.

### Common False Positive Detections

- **IDP.Generic** - Generic heuristic detection (Avast/AVG)
- **Win64:Malware-gen** - Generic 64-bit malware detection (Avast/AVG)
- **Trojan:Win32/Wacatac** - Generic detection (Windows Defender)

---

## For Users: How to Install

### Option 1: Add Exception Before Installing

1. Open your antivirus software
2. Go to Settings > Exceptions/Exclusions
3. Add the installation folder: `C:\Users\<YourName>\Dropbox\GenizahSearchPro\` (or your chosen location)
4. Add the temp folder: `C:\Users\<YourName>\AppData\Local\Temp\`
5. Run the installer

### Option 2: Temporarily Disable Real-Time Protection

1. Temporarily disable your antivirus real-time protection
2. Install the application
3. Add the installed folder to exceptions
4. Re-enable real-time protection

### Option 3: Report as False Positive

If the application is quarantined:
1. Open your antivirus software
2. Go to Quarantine/Virus Chest
3. Find GenizahSearchPro files
4. Select "Restore and add to exceptions"
5. Report as false positive (see links below)

---

## For Developers: Submitting for Whitelisting

### Avast/AVG False Positive Submission

1. Go to: https://www.avast.com/false-positive-file-form.php
2. Fill in the form:
   - **Email**: Your email address
   - **File**: Upload `GenizahSearchPro.exe` or the installer
   - **Reason**: "This is a legitimate open-source academic research tool for searching Cairo Genizah manuscripts. Built with PyInstaller and PyQt6. Source code: https://github.com/gershuni/GenizahSearch"
3. Submit and wait for response (usually 1-3 business days)

### Windows Defender False Positive Submission

1. Go to: https://www.microsoft.com/en-us/wdsi/filesubmission
2. Sign in with a Microsoft account
3. Select "Software developer" as submission type
4. Upload the file and provide details
5. Include link to GitHub repository

### VirusTotal Analysis

Before each release, scan the executable on VirusTotal:
1. Go to: https://www.virustotal.com/
2. Upload the executable
3. Document which engines flag it
4. Submit false positive reports to those vendors

### Vendor-Specific Links

| Vendor | False Positive Submission URL |
|--------|-------------------------------|
| Avast/AVG | https://www.avast.com/false-positive-file-form.php |
| Microsoft Defender | https://www.microsoft.com/en-us/wdsi/filesubmission |
| Kaspersky | https://opentip.kaspersky.com/ |
| Bitdefender | https://www.bitdefender.com/submit/ |
| ESET | https://support.eset.com/en/kb141 |
| Norton | https://submit.norton.com/ |
| McAfee | https://www.mcafee.com/enterprise/en-us/threat-center/false-positive.html |

---

## Code Signing (Recommended for Releases)

The most effective way to prevent false positives is to sign the executable with a valid code signing certificate. Options include:

1. **Standard Code Signing Certificate** (~$200-400/year)
   - DigiCert, Sectigo, GlobalSign
   - Establishes publisher identity

2. **Extended Validation (EV) Code Signing** (~$400-600/year)
   - Immediate SmartScreen reputation
   - Hardware token required

### Signing the Executable

```batch
signtool sign /f certificate.pfx /p password /tr http://timestamp.digicert.com /td sha256 /fd sha256 GenizahSearchPro.exe
```

---

## Build Optimizations Already Applied

The build script includes these optimizations to reduce false positives:

1. **Version Info Embedded**: `--version-file "version_info.txt"` adds Windows version information to the executable, making it appear more legitimate.

2. **No UPX Compression**: `--noupx` flag prevents UPX compression which often triggers antivirus heuristics.

3. **Clean Build**: `--clean` ensures no cached/corrupted files are included.

---

## Verifying the Application is Safe

Users can verify the application is safe by:

1. **Checking the Source Code**: The complete source code is available on GitHub
2. **Building from Source**: Follow the build instructions in README.md
3. **Checking VirusTotal**: Most major antivirus engines will show clean results
4. **Reviewing the Code**: The application only accesses:
   - Local files (for search indexes)
   - Google Gemini API (for AI features)
   - National Library of Israel API (for manuscript data)

---

## Contact

If you have questions about the application's safety, please open an issue on GitHub:
https://github.com/gershuni/GenizahSearch/issues
