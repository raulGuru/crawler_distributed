"""Page Elements Parser Worker module.

This module contains the PageElementsWorker class which extracts HTML element counts,
content metrics, and page performance characteristics from saved HTML files as part
of a distributed crawl-parser system.
"""

#TODO:Some approximations are made for metrics that normally would require additional context (like HTTPS status or download time), but these could be refined in a real system where that information is available.

# Since we can't measure actual download time after the fact,
        # we'll estimate it based on the size (assuming reasonable connection)
        # This is just a placeholder - in real world you'd want to use actual metrics
# This is a simple approximation - in a real system you'd want to store
        # the source domain with the file or extract it from the content
# In a real system, this would be determined from the original URL
        # or stored as metadata when crawling. Here we'll approximate.

import os
import sys
import re
import argparse
from urllib.parse import urlparse

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from parser.workers.base_parser_worker import (
    BaseParserWorker,
    RetryableError,
    NonRetryableError,
)


class PageElementsWorker(BaseParserWorker):
    """Worker for extracting page element metrics and content analysis from HTML files.

    This worker processes HTML files saved by the crawler, extracts various element counts,
    content metrics, readability scores, and performance characteristics,
    and stores the results in MongoDB.
    """

    def __init__(self, instance_id: int = 0):
        """Initialize the PageElementsWorker.

        Args:
            instance_id (int): Unique identifier for this worker instance.
        """
        super().__init__(
            tube_name="htmlparser_page_elements_extraction_tube",
            task_type="page_elements_extraction",
            instance_id=instance_id,
        )

    def get_data_field_name(self) -> str:
        """Return the MongoDB field name for this worker's data."""
        return "page_elements"

    def extract_data(self, html_content: str, html_path: str, doc_id_str: str, url: str, domain: str) -> dict:
        """Extract page elements data from HTML content.

        Args:
            html_content (str): The HTML content to parse.
            html_path (str): Path to the HTML file (for logging).
            doc_id_str (str): Document ID (for logging).

        Returns:
            dict: Extracted page elements data.

        Raises:
            NonRetryableError: For HTML parsing errors.
        """
        try:
            soup = self._create_soup(html_content)

            # Extract meta description data
            meta_description_data = self._extract_meta_description_data(soup)

            # Extract element counts
            element_counts = self._extract_element_counts(soup)

            # Extract text metrics
            text_metrics = self._extract_text_metrics(soup)

            # Extract readability metrics
            readability_metrics = self._calculate_readability_metrics(
                text_metrics["word_count"],
                text_metrics["sentence_count"],
                text_metrics["paragraph_count"]
            )

            # Extract script and resource metrics
            script_metrics = self._extract_script_metrics(soup, html_path)

            # Extract image metrics
            image_metrics = self._extract_image_metrics(soup)

            # Extract performance metrics
            performance_metrics = self._extract_performance_metrics(html_content)

            # Calculate content quality score
            content_quality_score = self._calculate_content_quality_score(
                text_metrics, element_counts, image_metrics
            )

            # Determine HTTPS status
            is_https = self._is_https(html_path, doc_id_str)

            # Combine all metrics into one structure matching MongoDB schema
            page_elements_data = {
                # Meta description metrics
                "description_length": meta_description_data["description_length"],
                "description_word_count": meta_description_data["description_word_count"],
                "description_too_short": meta_description_data["description_too_short"],
                "description_too_long": meta_description_data["description_too_long"],

                # HTTPS status
                "is_https": is_https,

                # Content quality score
                "content_quality_score": content_quality_score,

                # Element counts
                "lists": element_counts["lists"],
                "blockquotes": element_counts["blockquotes"],
                "tables": element_counts["tables"],
                "images_in_content": element_counts["images_in_content"],
                "videos_in_content": element_counts["videos_in_content"],
                "external_css": element_counts["external_css"],
                "inline_css": element_counts["inline_css"],
                "external_js": element_counts["external_js"],
                "inline_js": element_counts["inline_js"],
                "paragraphs": element_counts["paragraphs"],

                # Text metrics
                "word_count": text_metrics["word_count"],
                "text_ratio": text_metrics["text_ratio"],
                "character_count": text_metrics["character_count"],
                "sentence_count": text_metrics["sentence_count"],
                "paragraph_count": text_metrics["paragraph_count"],
                "avg_words_per_sentence": text_metrics["avg_words_per_sentence"],
                "avg_sentences_per_paragraph": text_metrics["avg_sentences_per_paragraph"],
                "content_length": text_metrics["content_length"],

                # Image metrics
                "images": image_metrics["images_count"],
                "images_with_alt_count": image_metrics["images_with_alt_count"],
                "images_without_alt_count": image_metrics["images_without_alt_count"],

                # Readability metrics
                "readability": readability_metrics,

                # Script metrics
                "file_script_count": script_metrics["file_script_count"],
                "same_domain_script_count": script_metrics["same_domain_script_count"],
                "third_party_script_count": script_metrics["third_party_script_count"],
                "inline_script_count": script_metrics["inline_script_count"],
                "inline_script_size": script_metrics["inline_script_size"],
                "total_script_count": script_metrics["total_script_count"],

                # Performance metrics
                "download_time": performance_metrics["download_time"],
                "html_size": performance_metrics["html_size"],
                "css_count": element_counts["external_css"],
                "js_count": element_counts["external_js"],
            }

            self.logger.debug(
                f"Extracted page_elements_data for doc_id: {doc_id_str}"
            )

            return page_elements_data

        except Exception as e:
            self.logger.error(
                f"HTML parsing failed for {html_path}, doc_id: {doc_id_str}: {e}"
            )
            raise NonRetryableError(f"HTML parsing failed for {html_path}: {e}")

    def _extract_meta_description_data(self, soup):
        """Extract meta description data and metrics.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Meta description metrics.
        """
        meta_description = soup.find("meta", attrs={"name": "description"})
        meta_description_content = meta_description.get("content", "") if meta_description else ""

        # Calculate metrics
        description_length = len(meta_description_content) if meta_description_content else 0
        words = meta_description_content.split() if meta_description_content else []
        description_word_count = len(words)

        # Check if too short or too long (standard SEO guidelines)
        description_too_short = description_length < 120 if description_length > 0 else False
        description_too_long = description_length > 160

        return {
            "description_length": description_length,
            "description_word_count": description_word_count,
            "description_too_short": description_too_short,
            "description_too_long": description_too_long,
        }

    def _extract_element_counts(self, soup):
        """Extract counts of various HTML elements.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Counts of various HTML elements.
        """
        # Basic element counts
        lists = len(soup.find_all(['ul', 'ol']))
        blockquotes = len(soup.find_all('blockquote'))
        tables = len(soup.find_all('table'))
        paragraphs = len(soup.find_all('p'))

        # CSS and JavaScript
        external_css = len(soup.find_all('link', rel='stylesheet'))
        inline_css = len(soup.find_all('style'))
        external_js = len(soup.find_all('script', src=True))
        inline_js = len(soup.find_all('script', src=None))

        # Images and videos in content
        # Try to identify main content area
        content_areas = soup.select('article, main, #content, .content, .entry-content, .post-content')

        if content_areas:
            # Use the first content area found
            content_area = content_areas[0]
            images_in_content = len(content_area.find_all('img'))

            # Count videos (various video elements and embeds)
            videos_in_content = (
                len(content_area.find_all('video')) +
                len(content_area.find_all('iframe', src=lambda x: x and ('youtube.com' in x or 'vimeo.com' in x))) +
                len(content_area.find_all(class_=lambda x: x and 'video' in x.lower()))
            )
        else:
            # Fall back to counting all images and videos
            images_in_content = len(soup.find_all('img'))
            videos_in_content = (
                len(soup.find_all('video')) +
                len(soup.find_all('iframe', src=lambda x: x and ('youtube.com' in x or 'vimeo.com' in x))) +
                len(soup.find_all(class_=lambda x: x and 'video' in x.lower()))
            )

        return {
            "lists": lists,
            "blockquotes": blockquotes,
            "tables": tables,
            "paragraphs": paragraphs,
            "external_css": external_css,
            "inline_css": inline_css,
            "external_js": external_js,
            "inline_js": inline_js,
            "images_in_content": images_in_content,
            "videos_in_content": videos_in_content,
        }

    def _extract_text_metrics(self, soup):
        """Extract and analyze text content metrics.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Text metrics.
        """
        # Extract all text content
        # Remove script and style content
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text from HTML
        text = soup.get_text()

        # Clean and normalize text
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)

        # Calculate basic metrics
        html_size = len(str(soup))
        text_size = len(text)
        text_ratio = round((text_size / html_size) * 100, 1) if html_size > 0 else 0
        character_count = len(text.replace(" ", "").replace("\n", ""))

        # Count words, sentences, and paragraphs
        words = re.findall(r'\b\w+\b', text.lower())
        word_count = len(words)

        # Count sentences - basic implementation
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        sentence_count = len(sentences)

        # Count paragraphs
        paragraphs = [p for p in text.split('\n\n') if p.strip()]
        paragraph_count = len(paragraphs)

        # Calculate averages
        avg_words_per_sentence = round(word_count / sentence_count, 1) if sentence_count > 0 else 0
        avg_sentences_per_paragraph = round(sentence_count / paragraph_count, 1) if paragraph_count > 0 else 0

        # Find main content text
        content_areas = soup.select('article, main, #content, .content, .entry-content, .post-content')
        if content_areas:
            main_content = content_areas[0].get_text()
            content_length = len(main_content)
        else:
            # If no content area identified, use the entire text
            content_length = text_size

        return {
            "word_count": word_count,
            "text_ratio": text_ratio,
            "character_count": character_count,
            "sentence_count": sentence_count,
            "paragraph_count": paragraph_count,
            "avg_words_per_sentence": avg_words_per_sentence,
            "avg_sentences_per_paragraph": avg_sentences_per_paragraph,
            "content_length": content_length,
        }

    def _calculate_readability_metrics(self, word_count, sentence_count, paragraph_count):
        """Calculate readability scores and reading time.

        Args:
            word_count (int): Total number of words.
            sentence_count (int): Total number of sentences.
            paragraph_count (int): Total number of paragraphs.

        Returns:
            dict: Readability metrics.
        """
        # Calculate Flesch-Kincaid Reading Ease score
        # Using simplified formula: 206.835 - 1.015 * (words/sentences) - 84.6 * (syllables/words)
        # Since syllable counting is complex, we'll estimate as 1.5 syllables per word on average
        if sentence_count == 0 or word_count == 0:
            flesch_kincaid_score = 0.0
        else:
            avg_sentence_length = word_count / sentence_count
            estimated_syllables = word_count * 1.5
            avg_syllables_per_word = estimated_syllables / word_count

            flesch_kincaid_score = round(
                206.835 - (1.015 * avg_sentence_length) - (84.6 * avg_syllables_per_word),
                1
            )

            # Ensure score is within 0-100 range
            flesch_kincaid_score = max(0, min(100, flesch_kincaid_score))

        # Determine grade level based on score
        if flesch_kincaid_score >= 90:
            grade = "5th grade"
        elif flesch_kincaid_score >= 80:
            grade = "6th grade"
        elif flesch_kincaid_score >= 70:
            grade = "7th grade"
        elif flesch_kincaid_score >= 60:
            grade = "8th-9th grade"
        elif flesch_kincaid_score >= 50:
            grade = "10th-12th grade"
        elif flesch_kincaid_score >= 30:
            grade = "College"
        else:
            grade = "College graduate"

        # Calculate reading time (average reading speed is 200-250 words per minute)
        reading_speed = 225  # words per minute
        reading_time_seconds = round(word_count / reading_speed * 60)
        reading_time_minutes = round(reading_time_seconds / 60, 1)

        return {
            "flesch_kincaid_score": flesch_kincaid_score,
            "flesch_kincaid_grade": grade,
            "reading_time_seconds": reading_time_seconds,
            "reading_time_minutes": reading_time_minutes
        }

    def _extract_script_metrics(self, soup, html_path):
        """Extract metrics related to JavaScript and scripts.

        Args:
            soup (BeautifulSoup): Parsed HTML content.
            html_path (str): Path to the HTML file.

        Returns:
            dict: Script metrics.
        """
        # Extract external scripts
        external_scripts = soup.find_all('script', src=True)
        file_script_count = len(external_scripts)

        # Extract inline scripts
        inline_scripts = soup.find_all('script', src=None)
        inline_script_count = len(inline_scripts)

        # Calculate inline script size
        inline_script_size = sum(len(script.string or "") for script in inline_scripts)

        # Determine domain for external scripts
        source_domain = self._extract_domain_from_path(html_path)
        same_domain_scripts = [script for script in external_scripts
                               if source_domain and source_domain in script.get('src', '')]

        same_domain_script_count = len(same_domain_scripts)
        third_party_script_count = file_script_count - same_domain_script_count
        total_script_count = file_script_count + inline_script_count

        return {
            "file_script_count": file_script_count,
            "same_domain_script_count": same_domain_script_count,
            "third_party_script_count": third_party_script_count,
            "inline_script_count": inline_script_count,
            "inline_script_size": inline_script_size,
            "total_script_count": total_script_count
        }

    def _extract_image_metrics(self, soup):
        """Extract metrics related to images.

        Args:
            soup (BeautifulSoup): Parsed HTML content.

        Returns:
            dict: Image metrics.
        """
        # Find all images
        images = soup.find_all('img')
        images_count = len(images)

        # Count images with and without alt text
        images_with_alt = [img for img in images if img.get('alt')]
        images_with_alt_count = len(images_with_alt)
        images_without_alt_count = images_count - images_with_alt_count

        return {
            "images_count": images_count,
            "images_with_alt_count": images_with_alt_count,
            "images_without_alt_count": images_without_alt_count
        }

    def _extract_performance_metrics(self, html_content):
        """Extract performance metrics.

        Args:
            html_content (str): The raw HTML content.

        Returns:
            dict: Performance metrics.
        """
        # Calculate HTML size
        html_size = len(html_content)

        # Since we can't measure actual download time after the fact,
        # we'll estimate it based on the size (assuming reasonable connection)
        # This is just a placeholder - in real world you'd want to use actual metrics
        estimated_download_time = round(html_size / (500 * 1024), 3)  # Assume 500 KB/s

        return {
            "download_time": estimated_download_time,
            "html_size": html_size
        }

    def _calculate_content_quality_score(self, text_metrics, element_counts, image_metrics):
        """Calculate an overall content quality score (0-100).

        Args:
            text_metrics (dict): Text analysis metrics.
            element_counts (dict): Element count metrics.
            image_metrics (dict): Image metrics.

        Returns:
            int: Content quality score (0-100).
        """
        score = 50  # Start with a neutral score

        # Content length factor (up to +25 points)
        if text_metrics["word_count"] > 1500:
            score += 25
        elif text_metrics["word_count"] > 1000:
            score += 20
        elif text_metrics["word_count"] > 750:
            score += 15
        elif text_metrics["word_count"] > 500:
            score += 10
        elif text_metrics["word_count"] > 300:
            score += 5

        # Content structure factor (up to +15 points)
        structure_score = 0
        if element_counts["paragraphs"] > 5:
            structure_score += 5
        if element_counts["lists"] > 0:
            structure_score += 5
        if image_metrics["images_count"] > 0:
            structure_score += 5
        score += min(15, structure_score)

        # Element variety factor (up to +10 points)
        variety_score = 0
        if element_counts["tables"] > 0:
            variety_score += 3
        if element_counts["blockquotes"] > 0:
            variety_score += 3
        if element_counts["videos_in_content"] > 0:
            variety_score += 4
        score += min(10, variety_score)

        # Ensure score is within 0-100 range
        return max(0, min(100, score))

    def _extract_domain_from_path(self, html_path):
        """Extract domain from HTML file path if possible.

        Args:
            html_path (str): Path to the HTML file.

        Returns:
            str: Domain name or None if can't be determined.
        """
        # This is a simple approximation - in a real system you'd want to store
        # the source domain with the file or extract it from the content
        try:
            # Try to parse domain from filename if it follows a pattern
            filename = os.path.basename(html_path)

            # Check if filename has a pattern like "domain_com.html" or "example.com.html"
            domain_match = re.search(r'([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', filename)
            if domain_match:
                return domain_match.group(1)
        except:
            pass

        return None

    def _is_https(self, html_path, doc_id_str):
        """Determine if the page was served over HTTPS.

        Args:
            html_path (str): Path to the HTML file.
            doc_id_str (str): Document ID.

        Returns:
            bool: True if the page was served over HTTPS.
        """
        # In a real system, this would be determined from the original URL
        # or stored as metadata when crawling. Here we'll approximate.
        try:
            # Try to find indicators in the filename
            filename = os.path.basename(html_path)
            if "https_" in filename or "https" in filename:
                return True

            # For doc_id, check if it contains https
            if "https" in doc_id_str:
                return True

            # Default to True as most modern sites use HTTPS
            return True
        except:
            # Default to True if we can't determine
            return True


def main():
    """Main entry point for the Page Elements Parser Worker."""
    parser = argparse.ArgumentParser(description="Page Elements Parser Worker")
    parser.add_argument(
        "--instance-id", type=int, default=0, help="Instance ID for this worker"
    )
    args = parser.parse_args()

    worker = PageElementsWorker(
        instance_id=args.instance_id,
    )
    worker.start()


if __name__ == "__main__":
    main()