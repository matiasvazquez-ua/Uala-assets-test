import io
import sys
import unittest
from pathlib import Path
from unittest import mock

from streamlit.testing.v1 import AppTest
from openpyxl import load_workbook


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

    def test_build_asset_payload_derives_company_from_country(self) -> None:
        row = {
            "Nombre del activo": "MON-01",
            "Hostname": "MON-01",
            "Serial Number": "SER-777",
            "Tipo de activo": "Monitores",
            "País": "Colombia",
        }
        _, attrs = app.build_asset_attributes_payload(row)
        attr_map = self._attr_map(attrs)

        self.assertEqual(attr_map[app.ID_PAIS], "Colombia")
        self.assertEqual(attr_map[app.ID_COMPANIA], "Bancar COL")

    def test_mass_update_identifier_accepts_multiple_aliases(self) -> None:
        self.assertEqual(app.resolve_mass_update_identifier({"Jira Key": "ISI-31645"}), "ISI-31645")
        self.assertEqual(app.resolve_mass_update_identifier({"Host name": "WKS-001"}), "WKS-001")

    def test_mass_upload_template_contains_expected_sheets_and_headers(self) -> None:
        raw = app.build_mass_upload_template_bytes()
        workbook = load_workbook(io.BytesIO(raw))

        self.assertEqual(workbook.sheetnames, ["Carga masiva", "Listas", "Instrucciones"])
        ws = workbook["Carga masiva"]
        headers = [ws.cell(row=1, column=idx).value for idx in range(1, len(app.MASS_UPLOAD_TEMPLATE_HEADERS) + 1)]

        self.assertEqual(headers, app.MASS_UPLOAD_TEMPLATE_HEADERS)
        self.assertEqual(ws["A2"].value, app.MASS_UPLOAD_TEMPLATE_EXAMPLE_ROW["Tipo de activo"])
        self.assertEqual(ws["G2"].value, app.MASS_UPLOAD_TEMPLATE_EXAMPLE_ROW["País"])
        self.assertEqual(workbook["Listas"].sheet_state, "hidden")

    def test_build_asset_create_payload_resolves_special_attribute_types(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        row = {
            "Nombre del activo": "NB-01",
            "Hostname": "NB-01",
            "Serial Number": "SER-001",
            "Tipo de activo": "laptops",
            "Estado del activo": "Stock nuevo",
            "Usuario asignado": "ana@bancar.com",
            "Compañía": "Bancar ARG",
        }
        attr_defs = [
            {"id": app.ID_NAME, "name": "Name", "defaultType": {"name": "Text"}},
            {"id": app.ID_HOSTNAME, "name": "Hostname", "defaultType": {"name": "Text"}},
            {"id": app.ID_SERIAL, "name": "Serial Number", "defaultType": {"name": "Text"}},
            {"id": app.ID_ESTADO, "name": "Estado del activo", "defaultType": {"name": "Status"}},
            {"id": app.ID_ASIGNACION, "name": "Usuario asignado", "defaultType": {"name": "User"}},
            {"id": app.ID_CATEGORIA, "name": "Categoria", "referenceObjectType": {"id": "1300-ref"}},
            {"id": app.ID_COMPANIA, "name": "Compañía", "referenceObjectType": {"id": "1337-ref"}},
        ]

        def fake_ref_resolver(_config, reference_type_id, raw_value, _auth, *, attr_id="", headers=None):
            if reference_type_id == "1300-ref":
                return "CAT-1"
            if reference_type_id == "1337-ref" and raw_value == "Bancar ARG":
                return "COM-1"
            return None

        def fake_option_lookup(_config, attr_id, _auth, _headers):
            return {"stock nuevo": "STATUS-1"} if attr_id == app.ID_ESTADO else {}

        with (
            mock.patch.object(app, "fetch_object_type_attributes", return_value=attr_defs),
            mock.patch.object(app, "resolve_reference_object_key", side_effect=fake_ref_resolver),
            mock.patch.object(app, "fetch_attribute_option_lookup", side_effect=fake_option_lookup),
            mock.patch.object(app, "resolve_user_account_id", return_value="acc-123"),
        ):
            type_id, attrs, issues = app.build_asset_create_payload(config, row)

        attr_map = self._attr_map(attrs)
        self.assertEqual(type_id, app.CATEGORY_TO_TYPE_ID["portatiles"])
        self.assertEqual(issues, [])
        self.assertEqual(attr_map[app.ID_ESTADO], "STATUS-1")
        self.assertEqual(attr_map[app.ID_ASIGNACION], "acc-123")
        self.assertEqual(attr_map[app.ID_CATEGORIA], "CAT-1")
        self.assertEqual(attr_map[app.ID_COMPANIA], "COM-1")

    def test_build_asset_create_payload_creates_missing_model_and_skips_unknown_assignment(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        row = {
            "Nombre del activo": "NB-02",
            "Hostname": "NB-02",
            "Serial Number": "SER-002",
            "Tipo de activo": "laptops",
            "Nombre del modelo": "MacBook Air M4 16GB 512GB",
            "Usuario asignado": "matias.vazquez@gmail.com",
        }
        attr_defs = [
            {"id": app.ID_NAME, "name": "Name", "defaultType": {"name": "Text"}},
            {"id": app.ID_HOSTNAME, "name": "Hostname", "defaultType": {"name": "Text"}},
            {"id": app.ID_SERIAL, "name": "Serial Number", "defaultType": {"name": "Text"}},
            {"id": app.ID_MODELO, "name": "Nombre del modelo", "referenceObjectType": {"id": "994-ref"}},
            {"id": app.ID_ASIGNACION, "name": "Asignacion", "defaultType": {"name": "User"}},
            {"id": app.ID_CATEGORIA, "name": "Categoria", "referenceObjectType": {"id": "1300-ref"}},
        ]

        def fake_ref_resolver(_config, reference_type_id, raw_value, _auth, *, attr_id="", headers=None):
            if reference_type_id == "1300-ref":
                return "CAT-1"
            if reference_type_id == "994-ref":
                return None
            return None

        with (
            mock.patch.object(app, "fetch_object_type_attributes", return_value=attr_defs),
            mock.patch.object(app, "resolve_reference_object_key", side_effect=fake_ref_resolver),
            mock.patch.object(app, "create_reference_object", return_value="MOD-123"),
            mock.patch.object(app, "fetch_attribute_option_lookup", return_value={}),
            mock.patch.object(app, "resolve_user_account_id", return_value=None),
        ):
            type_id, attrs, issues = app.build_asset_create_payload(config, row)

        attr_map = self._attr_map(attrs)
        self.assertEqual(type_id, app.CATEGORY_TO_TYPE_ID["portatiles"])
        self.assertEqual(issues, [])
        self.assertEqual(attr_map[app.ID_MODELO], "MOD-123")
        self.assertNotIn(app.ID_ASIGNACION, attr_map)

    def test_build_asset_create_payload_keeps_assignment_raw_when_schema_is_not_user_type(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        row = {
            "Nombre del activo": "NB-03",
            "Hostname": "NB-03",
            "Serial Number": "SER-003",
            "Tipo de activo": "laptops",
            "Usuario asignado": "matias.vazquez@gmail.com",
        }
        attr_defs = [
            {"id": app.ID_NAME, "name": "Name", "defaultType": {"name": "Text"}},
            {"id": app.ID_HOSTNAME, "name": "Hostname", "defaultType": {"name": "Text"}},
            {"id": app.ID_SERIAL, "name": "Serial Number", "defaultType": {"name": "Text"}},
            {"id": app.ID_CATEGORIA, "name": "Categoria", "referenceObjectType": {"id": "1300-ref"}},
            {"id": app.ID_ASIGNACION, "name": "Asignacion", "defaultType": {"name": "Text"}},
        ]

        def fake_ref_resolver(_config, reference_type_id, raw_value, _auth, *, attr_id="", headers=None):
            if reference_type_id == "1300-ref":
                return "CAT-1"
            return None

        with (
            mock.patch.object(app, "fetch_object_type_attributes", return_value=attr_defs),
            mock.patch.object(app, "resolve_reference_object_key", side_effect=fake_ref_resolver),
            mock.patch.object(app, "fetch_attribute_option_lookup", return_value={}),
            mock.patch.object(app, "resolve_user_account_id") as resolve_user_mock,
        ):
            type_id, attrs, issues = app.build_asset_create_payload(config, row)

        attr_map = self._attr_map(attrs)
        self.assertEqual(type_id, app.CATEGORY_TO_TYPE_ID["portatiles"])
        self.assertEqual(issues, [])
        self.assertEqual(attr_map[app.ID_ASIGNACION], "matias.vazquez@gmail.com")
        resolve_user_mock.assert_not_called()

    def test_detects_template_example_row(self) -> None:
        self.assertTrue(app.is_mass_upload_example_row(dict(app.MASS_UPLOAD_TEMPLATE_EXAMPLE_ROW)))
        altered = dict(app.MASS_UPLOAD_TEMPLATE_EXAMPLE_ROW)
        altered["Serial Number"] = "OTRO-SERIAL"
        self.assertFalse(app.is_mass_upload_example_row(altered))

    def test_resolve_reference_object_key_matches_email_inside_label(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        lookup = {
            app.normalize_lookup_key("Matias Vazquez (matias.vazquez@gmail.com)"): "USR-1",
        }
        with mock.patch.object(app, "fetch_reference_object_lookup", return_value=lookup):
            resolved = app.resolve_reference_object_key(
                config,
                "1232-ref",
                "matias.vazquez@gmail.com",
                _auth := mock.Mock(),
                attr_id=app.ID_ASIGNACION,
                headers={},
            )
        self.assertEqual(resolved, "USR-1")

    def test_warranty_date_is_sent_as_jira_datetime(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        row = {
            "Nombre del activo": "NB-04",
            "Hostname": "NB-04",
            "Serial Number": "SER-004",
            "Tipo de activo": "laptops",
            "Fecha garantía": "2027-03-20",
        }
        attr_defs = [
            {"id": app.ID_NAME, "name": "Name", "defaultType": {"name": "Text"}},
            {"id": app.ID_HOSTNAME, "name": "Hostname", "defaultType": {"name": "Text"}},
            {"id": app.ID_SERIAL, "name": "Serial Number", "defaultType": {"name": "Text"}},
            {"id": app.ID_CATEGORIA, "name": "Categoria", "referenceObjectType": {"id": "1300-ref"}},
            {"id": app.ID_FECHA_GARANTIA, "name": "Fecha soporte garantia", "defaultType": {"name": "DateTime"}},
        ]

        def fake_ref_resolver(_config, reference_type_id, raw_value, _auth, *, attr_id="", headers=None):
            if reference_type_id == "1300-ref":
                return "CAT-1"
            return None

        with (
            mock.patch.object(app, "fetch_object_type_attributes", return_value=attr_defs),
            mock.patch.object(app, "resolve_reference_object_key", side_effect=fake_ref_resolver),
            mock.patch.object(app, "fetch_attribute_option_lookup", return_value={}),
        ):
            _, attrs, issues = app.build_asset_create_payload(config, row)

        attr_map = self._attr_map(attrs)
        self.assertEqual(issues, [])
        self.assertEqual(attr_map[app.ID_FECHA_GARANTIA], "2027-03-20T00:00:00.000Z")

    def test_create_asset_from_payload_returns_real_error_body(self) -> None:
        config = app.AppConfig(
            jira_email="jira@example.com",
            jira_api_token="token",
            workspace_id="workspace",
            site="https://bancar.atlassian.net",
            openai_api_key="",
            openai_model="gpt-4o-mini",
            rovo_api_key="",
            rovo_enabled=False,
        )
        fake_response = mock.Mock(status_code=404, text='{"error":"not found"}')
        fake_response.json.return_value = {}
        fake_client = mock.MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.post.return_value = fake_response

        with mock.patch.object(app.httpx, "Client", return_value=fake_client):
            ok, msg = app.create_asset_from_payload(
                config,
                "213",
                [{"objectTypeAttributeId": app.ID_NAME, "objectAttributeValues": [{"value": "NB-01"}]}],
            )

        self.assertFalse(ok)
        self.assertIn("404", msg)
        self.assertIn("not found", msg)


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
