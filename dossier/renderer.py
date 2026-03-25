"""
Dossier renderer — generates HTML and PDF output from analysis results.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

from jinja2 import Template

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.dirname(__file__)


class DossierRenderer:
    """Renders the Creative Intelligence Dossier as HTML or PDF."""

    def __init__(self, analysis_results: dict, asin: str):
        self.data = analysis_results
        self.asin = asin
        self.sections = analysis_results.get("sections", {})

    def _load_template(self) -> Template:
        template_path = os.path.join(TEMPLATE_DIR, "template.html")
        with open(template_path, "r") as f:
            return Template(f.read())

    def _prepare_context(self) -> dict:
        """Prepare template context from analysis results."""
        s = self.sections

        # Helper to safely get nested values
        def safe_get(d, *keys, default=None):
            for k in keys:
                if isinstance(d, dict):
                    d = d.get(k, default)
                else:
                    return default
            return d

        return {
            "asin": self.asin,
            "generated_at": datetime.now().strftime("%B %d, %Y at %I:%M %p"),
            "total_reviews": self.data.get("total_reviews", 0),
            "avg_rating": self.data.get("avg_rating", 0),
            "star_distribution": self.data.get("star_distribution", {}),

            # Section 1
            "market_snapshot": s.get("market_snapshot", {}),

            # Section 2
            "avatar_monologue": safe_get(s, "avatar_monologue", "monologue", default=""),
            "avatar_phrases": safe_get(s, "avatar_monologue", "key_phrases_used", default=[]),

            # Section 3
            "language_bible": s.get("language_bible", {}),

            # Section 4
            "headlines": safe_get(s, "headline_bank", "headlines", default=[]),

            # Section 5
            "objection_sequence": safe_get(s, "objection_sequence", "objection_sequence", default=[]),

            # Section 6
            "angle_matrix": safe_get(s, "angle_matrix", "matrix", default=[]),

            # Section 7
            "proof_architecture": s.get("proof_architecture", {}),

            # Section 8
            "competitive_map": s.get("competitive_map", {}),

            # Section 9
            "conversion_blueprint": s.get("conversion_blueprint", {}),

            # Section 10
            "creative_briefs": safe_get(s, "creative_briefs", "briefs", default=[]),

            # ML data
            "clustering": self.data.get("clustering", {}),
            "ngrams": self.data.get("ngrams", {}),

            # JSON helper
            "json_dumps": json.dumps,
        }

    def render_html(self, output_path: str) -> str:
        """Render the dossier as a standalone HTML file."""
        template = self._load_template()
        context = self._prepare_context()
        html = template.render(**context)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info(f"HTML dossier saved to {output_path}")
        return output_path

    def render_pdf(self, output_path: str) -> str:
        """Render the dossier as a PDF via weasyprint."""
        # First render HTML
        html_path = output_path.replace(".pdf", "_temp.html")
        self.render_html(html_path)

        try:
            from weasyprint import HTML
            HTML(filename=html_path).write_pdf(output_path)
            logger.info(f"PDF dossier saved to {output_path}")
        except (ImportError, OSError) as e:
            logger.error(f"PDF generation failed: {e}")
            logger.error("For PDF support, install system deps: brew install pango gobject-introspection")
            raise
        finally:
            # Clean up temp HTML
            try:
                os.remove(html_path)
            except OSError:
                pass

        return output_path
