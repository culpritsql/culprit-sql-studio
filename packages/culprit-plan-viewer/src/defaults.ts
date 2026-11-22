import enginesJson from "../../../app/src/gen/engines.json";
import cardinalsJson from "../../../locales/.cldr/cardinals.json";
import englishJson from "../../../locales/en.json";
import type { Engine, EngineFacts } from "./types.js";

export type ViewerEngineSpec = {
  label: string;
  abilities: { plan_estimates: boolean };
  products: { id: string; label: string; prefix: string | null }[];
  evidence_presentation: { generic_plan_context: string };
};

export type ViewerLocaleMeta = {
  plural: string;
  decimal: string;
  group: string;
  direction?: "ltr" | "rtl";
};

export type CardinalRelation = [string, number, boolean, [number, number][]];
export type PluralCategory = "zero" | "one" | "two" | "few" | "many" | "other";
export type CardinalCase = [PluralCategory, CardinalRelation[][]];

const rawEngines = enginesJson as unknown as Record<Engine, EngineFacts>;

export const DEFAULT_ENGINES: Record<Engine, ViewerEngineSpec> = Object.fromEntries(
  Object.entries(rawEngines).map(([key, spec]) => [
    key,
    {
      label: spec.label,
      abilities: { plan_estimates: Boolean(spec.abilities.plan_estimates) },
      products: spec.products.map((p) => ({ id: p.id, label: p.label, prefix: p.prefix })),
      evidence_presentation: { generic_plan_context: spec.evidence_presentation.generic_plan_context },
    },
  ]),
) as Record<Engine, ViewerEngineSpec>;

const rawEnglish = englishJson as unknown as { _meta: ViewerLocaleMeta } & Record<string, string>;
export const DEFAULT_LOCALE: ViewerLocaleMeta = rawEnglish._meta;
export const DEFAULT_MESSAGES: Record<string, string> = rawEnglish;
export const CARDINAL_LANGUAGES = cardinalsJson.languages as Record<string, number>;
export const CARDINAL_RULES = cardinalsJson.rules as unknown as CardinalCase[][];
