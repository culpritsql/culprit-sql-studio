//! Target-language coverage uses the same facts and one renderer; no translated string selects a branch.
use super::*;

#[test]
fn localized_complete_sentences_keep_timing_units_and_the_native_read_unit_fact() {
    for (language, cheap, slow) in [
        (
            "en",
            "The plan as a whole is cheap, so no share of its cost is put on one step.",
            "The plan is estimated to be cheap, yet its one run took 1.5 s: the time isn't in what it costs, so no share of its cost is put on one step.",
        ),
        (
            "es",
            "El plan en su conjunto es de bajo coste, por lo que no se atribuye ninguna proporción de su coste a un paso.",
            "Se estima que el plan es de bajo coste, pero su única ejecución tardó 1,5 s: el tiempo no está reflejado en su coste, por lo que no se atribuye ninguna proporción a un paso.",
        ),
        (
            "fr",
            "Le plan dans son ensemble est peu coûteux, aucune part de son coût n’est donc attribuée à une étape.",
            "Le plan est estimé peu coûteux, pourtant son unique exécution a duré 1,5 s\u{a0}: le temps n’est pas reflété dans son coût, aucune part de ce coût n’est donc attribuée à une étape.",
        ),
        (
            "de",
            "Der Plan ist insgesamt kostengünstig, daher wird kein Anteil seiner Kosten einem Schritt zugeschrieben.",
            "Der Plan wird als kostengünstig geschätzt, doch seine einzige Ausführung dauerte 1,5 s: Die Zeit spiegelt sich nicht in seinen Kosten wider, daher wird kein Kostenanteil einem Schritt zugeschrieben.",
        ),
    ] {
        culprit_core::i18n::scope(language, || {
            let native_plan = plan(Engine::Mssql, 0.01);
            assert_eq!(cheap_plan_reason(&native_plan, None), cheap);
            assert_eq!(cheap_plan_reason(&native_plan, Some(&slow_query(1.0, 1500.0, 1500.0, None))), slow);
            assert_eq!(read_unit_for_message(&native_plan), "pages");
            assert_eq!(read_unit_for_message(&plan(Engine::Mysql, 0.01)), "rows_examined");
        });
    }
}

#[test]
fn decimal_row_grammar_is_native_cldr_and_a_missing_table_is_never_rebound_to_a_label() {
    for (language, rows, noun, unnamed) in [
        ("es", 1.04, "las 1,0 filas", "su tabla"),
        ("fr", 0.96, "1,0 ligne", "sa table"),
        ("de", 1.04, "1,0 Zeilen", "seiner Tabelle"),
    ] {
        culprit_core::i18n::scope(language, || {
            let mut plan = plan(Engine::Postgres, 1.0);
            plan.root.op = "Seq Scan".into();
            plan.root.kind = NodeKind::Scan;
            plan.root.object = None;
            plan.root.table_rows = Some(rows);
            let text = whole_read_reason(&plan, &plan.root);
            assert!(text.contains("(1,00)"));
            assert!(text.contains("`Seq Scan`"));
            assert!(text.contains(noun), "{language}: {text}");
            assert!(text.contains(unnamed));
            assert!(!text.contains("public.items"));
            assert!(!text.contains("view.plan_shares."));
        });
    }
}
