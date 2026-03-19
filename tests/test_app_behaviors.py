import sys
import unittest
from pathlib import Path

from streamlit.testing.v1 import AppTest


APP_DIR = Path(__file__).resolve().parents[1]
APP_FILE = APP_DIR / "app.py"
sys.path.insert(0, str(APP_DIR))

import app  # noqa: E402


class TestPromptParsing(unittest.TestCase):
    def test_dashboard_request_supports_natural_language_without_keyword(self) -> None:
        request = app.parse_nl_dashboard_request("mostrame gasto por país y calidad de datos para Bancar ARG")
        self.assertTrue(request)
        self.assertTrue(request["show_spend"])
        self.assertTrue(request["show_geo"])
        self.assertTrue(request["show_quality"])
        self.assertEqual(request["filters"].get("company"), "Bancar ARG")
        self.assertNotIn("country", request["filters"])

    def test_compare_stock_prompt_does_not_invent_filters(self) -> None:
        filters = app.parse_filters_from_prompt("comparar stock entre países")
        self.assertEqual(filters, {})

    def test_bulk_update_accepts_natural_phrase(self) -> None:
        parsed = app.parse_bulk_location_action("actualizá en lote ISI-31645 ISI-32067 a México Bancar MEX")
        self.assertEqual(parsed, (["ISI-31645", "ISI-32067"], "Bancar MEX", "México"))

    def test_attribute_search_does_not_pollute_filters(self) -> None:
        self.assertEqual(app.detect_attribute_search("activos donde Nombre del modelo contiene thinkpad"), ("Nombre del modelo", "contiene", "thinkpad"))
        self.assertEqual(app.parse_filters_from_prompt("activos donde Nombre del modelo contiene thinkpad"), {})


class TestScriptInputs(unittest.TestCase):
    def _attr_map(self, attrs: list[dict]) -> dict[str, str]:
        out: dict[str, str] = {}
        for row in attrs:
            values = row.get("objectAttributeValues") or []
            if values:
                out[str(row.get("objectTypeAttributeId"))] = str(values[0].get("value", ""))
        return out

    def test_build_asset_payload_accepts_flexible_headers(self) -> None:
        row = {
            "Nombre del activo": "NB-01",
            "Host name": "WKS-001",
            "Model": "ThinkPad X1",
            "Fecha de compra": "2026-01-01",
            "Estado del activo": "En uso",
            "Entidad del activo": "IT",
            "Fecha garantía": "2027-01-01",
            "Purchase Price": "1500",
            "Serial Number": "SER-001",
            "País": "México",
            "Usuario asignado": "ana@bancar.com",
            "Provider": "Lenovo",
            "Category": "laptops",
            "Compañía": "Bancar MEX",
        }
        type_id, attrs = app.build_asset_attributes_payload(row)
        attr_map = self._attr_map(attrs)

        self.assertEqual(type_id, app.CATEGORY_TO_TYPE_ID["portatiles"])
        self.assertEqual(attr_map[app.ID_NAME], "NB-01")
        self.assertEqual(attr_map[app.ID_HOSTNAME], "WKS-001")
        self.assertEqual(attr_map[app.ID_MODELO], "ThinkPad X1")
        self.assertEqual(attr_map[app.ID_PAIS], "México")
        self.assertEqual(attr_map[app.ID_COMPANIA], "Bancar MEX")
        self.assertEqual(attr_map[app.ID_ASIGNACION], "ana@bancar.com")

    def test_build_asset_payload_uses_tipo_when_category_is_missing(self) -> None:
        row = {
            "Nombre": "Dock-01",
            "Tipo": "tablets",
            "Hostname": "TAB-001",
        }
        type_id, attrs = app.build_asset_attributes_payload(row)
        attr_map = self._attr_map(attrs)

        self.assertEqual(type_id, app.CATEGORY_TO_TYPE_ID["tablets"])
        self.assertEqual(attr_map[app.ID_NAME], "Dock-01")
        self.assertEqual(attr_map[app.ID_HOSTNAME], "TAB-001")

    def test_mass_update_identifier_accepts_multiple_aliases(self) -> None:
        self.assertEqual(app.resolve_mass_update_identifier({"Jira Key": "ISI-31645"}), "ISI-31645")
        self.assertEqual(app.resolve_mass_update_identifier({"Host name": "WKS-001"}), "WKS-001")


class TestChatDashboardIntegration(unittest.TestCase):
    def test_chat_renders_dashboard_section_below_history(self) -> None:
        at = AppTest.from_file(str(APP_FILE), default_timeout=180)
        at.query_params["page"] = "Chat"
        at.run()
        at.chat_input[0].set_value("mostrame gasto por país y calidad de datos para Bancar ARG").run()

        markdown_values = [str(getattr(item, "value", "")) for item in at.markdown]
        self.assertEqual(len(at.exception), 0)
        self.assertGreaterEqual(len(at.chat_message), 2)
        self.assertTrue(any("### Dashboard solicitado" in value for value in markdown_values))
        self.assertGreaterEqual(sum("**Dashboard —" in value for value in markdown_values), 2)


if __name__ == "__main__":
    unittest.main()
