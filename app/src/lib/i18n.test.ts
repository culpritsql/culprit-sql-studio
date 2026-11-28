import { expect, test } from "bun:test";
import { matchCatalogTag, registerCatalogs, setLanguage, t } from "./i18n";

test("English fallback keeps its punctuation and reading direction inside an RTL language", () => {
  registerCatalogs({ ar: { _meta: { name: "العربية", english_name: "Arabic", plural: "ar", decimal: ".", group: ",", direction: "rtl" }, "ui.common.copy": "نسخ" } });
  try {
    setLanguage("ar");
    expect(t("ui.common.copy")).toBe("نسخ");
    expect(t("ui.sidebar.language")).toBe("\u2066Language\u2069");
  } finally {
    setLanguage("en");
  }
  expect(t("ui.sidebar.language")).toBe("Language");
});

test("System locale tags resolve canonical script and regional catalogs", () => {
  const available = new Set(["en", "de", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh-Hans"]);
  expect(matchCatalogTag("zh-CN", available)).toBe("zh-Hans");
  expect(matchCatalogTag("zh-Hans-CN", available)).toBe("zh-Hans");
  expect(matchCatalogTag("zh", available)).toBe("zh-Hans");
  expect(matchCatalogTag("zh-TW", available)).toBeUndefined();
  expect(matchCatalogTag("pt-BR", available)).toBe("pt-BR");
  expect(matchCatalogTag("pt", available)).toBe("pt-BR");
  expect(matchCatalogTag("ja-JP", available)).toBe("ja");
  expect(matchCatalogTag("ko-KR", available)).toBe("ko");
  expect(matchCatalogTag("ru-RU", available)).toBe("ru");
});
