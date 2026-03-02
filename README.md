# Atlassian Ticket Scraper

**Author:** Giuseppe Bosi

## Description

A professional Python application designed to incrementally scrape tickets from Atlassian Service Desk portals. This tool automates the process of collecting ticket data while maintaining efficiency through incremental updates and avoiding duplicate entries.

## Features

- **Incremental Scraping:** Only fetches new tickets since the last run
- **Automated Login:** Handles authentication to Atlassian Service Desk
- **Data Management:** Merges new tickets with existing data while preventing duplicates
- **Configurable:** Environment-based configuration for security and flexibility
- **Robust Error Handling:** Graceful handling of network issues and page loading

## Prerequisites

- Python 3.7 or higher
- Chrome browser installed
- Valid Atlassian Service Desk credentials

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables by copying `.env.example` to `.env` and filling in your credentials:
   ```bash
   cp .env.example .env
   ```

## Configuration

Create a `.env` file with the following variables:

```env
# Google Drive base path for ticket storage
GDRIVE_BASE_PATH=your_google_drive_path

# Atlassian site configuration
ATLASSIAN_SITE=your_atlassian_site
EMAIL=your_email@domain.com
PASSWORD=your_password
```

## Usage

Run the scraper with:

```bash
python "scrap2_incremental r2 release.py"
```

The script will:
1. Authenticate to your Atlassian Service Desk
2. Scrape tickets incrementally (only new ones)
3. Save data to a CSV file in your specified Google Drive path
4. Maintain proper sorting and deduplication

## Output

The scraper generates a `tickets.csv` file containing:
- Ticket key and reference
- Summary and status
- Type and service desk
- Requester information
- Creation and update timestamps
- Priority levels
- Direct URLs

## Security Considerations

- Credentials are stored in environment variables, not in the code
- The `.env` file should never be committed to version control
- Chrome WebDriver handles browser automation securely

## Dependencies

- selenium: Web browser automation
- pandas: Data manipulation and CSV handling
- beautifulsoup4: HTML parsing
- webdriver-manager: Automatic ChromeDriver management
- python-dotenv: Environment variable loading

## License

This project is proprietary software. All rights reserved.

## Support

For technical support or inquiries, please contact the author directly.

---

*Note: This tool is designed for legitimate business purposes only. Users must ensure they have proper authorization to access the target Atlassian Service Desk instances.*
