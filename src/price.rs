//! Per-attempt price ceiling, sent upstream as bid headers.
//!
//! The upstream picks a provider by score, and with no bid attached it is free
//! to pick the most expensive tier it is allowed to use. That is not a theory:
//! on 2026-10-23 it served the same model from a tier priced at 49.8% of
//! official for six and a half hours, against the 0.1-1.8% we normally pay, on
//! unchanged traffic. Eleven dollars of a thirteen-dollar day went out in that
//! window.
//!
//! A bid is a ceiling, not a discount. A request served under a bid is billed
//! exactly what it would have been billed without one, so the ceiling costs
//! nothing while cheap capacity exists. What it changes is the failure
//! direction: when every provider under the bid is busy, the upstream answers
//! `402 no_provider_under_bid` instead of quietly upgrading us to a tier we
//! never agreed to.
//!
//! `MAX_INPUT_PRICE` and `MAX_OUTPUT_PRICE` are absolute USD per million
//! tokens. The defaults, `0.003` and `0.012`, are 2% of the official $0.15 /
//! $0.60: about 20x the cheapest tier, and 25x cheaper than the tier that
//! burned us. `0` sends no header, the same convention `Caps` and
//! `InputLimit` use.
//!
//! The account-level discount floor is a wallet backstop, not a service
//! control: an explicit bid overrides it, and its own rejection arrives as a
//! 402, which is a status the proxy would otherwise read as a dead key.

/// Header naming the highest input price per million tokens we accept.
pub const INPUT_HEADER: &str = "x-max-input-price";

/// Header naming the highest output price per million tokens we accept.
pub const OUTPUT_HEADER: &str = "x-max-output-price";

/// Default input ceiling: 2% of the official $0.15 per million tokens.
pub const DEFAULT_MAX_INPUT_PRICE: f64 = 0.003;

/// Default output ceiling: 2% of the official $0.60 per million tokens.
pub const DEFAULT_MAX_OUTPUT_PRICE: f64 = 0.012;

/// The ceiling one upstream attempt carries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PriceCeiling {
    /// Highest input price per million tokens, in USD. `0` sends no header.
    pub max_input: f64,
    /// Highest output price per million tokens, in USD. `0` sends no header.
    pub max_output: f64,
}

impl PriceCeiling {
    /// Read the ceiling from the environment.
    pub fn from_env() -> Self {
        Self::parse(
            std::env::var("MAX_INPUT_PRICE").ok().as_deref(),
            std::env::var("MAX_OUTPUT_PRICE").ok().as_deref(),
        )
    }

    /// Parse the two prices. Unset, unparsable or negative values fall back to
    /// the default for that field, so a typo tightens the ceiling to the safe
    /// value instead of turning it off.
    pub fn parse(input: Option<&str>, output: Option<&str>) -> Self {
        Self {
            max_input: read_price(input, DEFAULT_MAX_INPUT_PRICE),
            max_output: read_price(output, DEFAULT_MAX_OUTPUT_PRICE),
        }
    }

    /// The headers one attempt carries, in the order they are applied. Empty
    /// when both ceilings are off.
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        let mut out = Vec::with_capacity(2);
        if self.max_input > 0.0 {
            out.push((INPUT_HEADER, format_price(self.max_input)));
        }
        if self.max_output > 0.0 {
            out.push((OUTPUT_HEADER, format_price(self.max_output)));
        }
        out
    }

    /// Attach the ceiling to one upstream attempt.
    pub fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = req;
        for (name, value) in self.headers() {
            req = req.header(name, value);
        }
        req
    }

    /// False when both ceilings are disabled with `0`.
    pub fn enabled(&self) -> bool {
        self.max_input > 0.0 || self.max_output > 0.0
    }

    /// The form the startup log prints.
    pub fn describe(&self) -> String {
        if !self.enabled() {
            return "off".to_string();
        }
        let input = if self.max_input > 0.0 {
            format!("${}/Mtok in", format_price(self.max_input))
        } else {
            "in off".to_string()
        };
        let output = if self.max_output > 0.0 {
            format!("${}/Mtok out", format_price(self.max_output))
        } else {
            "out off".to_string()
        };
        format!("{input}, {output}")
    }
}

fn read_price(raw: Option<&str>, default: f64) -> f64 {
    match raw.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() && v >= 0.0 => v,
            _ => default,
        },
        None => default,
    }
}

/// Shortest round-tripping form, so `0.003` is sent as `0.003` and not as a
/// float artifact. A whole number keeps one decimal place, because `1` and
/// `1.0` are the same bid but the latter reads as a price.
fn format_price(value: f64) -> String {
    let rendered = format!("{value}");
    if rendered.contains('.') || rendered.contains('e') {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_ceiling_sends_both_headers() {
        let ceiling = PriceCeiling::parse(Some("0.003"), Some("0.012"));
        let headers = ceiling.headers();
        assert_eq!(
            headers,
            vec![
                (INPUT_HEADER, "0.003".to_string()),
                (OUTPUT_HEADER, "0.012".to_string()),
            ]
        );
        assert!(ceiling.enabled());
    }

    #[test]
    fn ceiling_disabled_sends_no_header() {
        let ceiling = PriceCeiling::parse(Some("0"), Some("0"));
        assert!(ceiling.headers().is_empty());
        assert!(!ceiling.enabled());
        assert_eq!(ceiling.describe(), "off");
    }

    #[test]
    fn one_sided_ceiling_sends_one_header() {
        let ceiling = PriceCeiling::parse(Some("0"), Some("0.012"));
        assert_eq!(
            ceiling.headers(),
            vec![(OUTPUT_HEADER, "0.012".to_string())]
        );
    }

    #[test]
    fn defaults_are_two_percent_of_official() {
        let ceiling = PriceCeiling::parse(None, None);
        assert_eq!(ceiling.max_input, DEFAULT_MAX_INPUT_PRICE);
        assert_eq!(ceiling.max_output, DEFAULT_MAX_OUTPUT_PRICE);
        // 2% of the official $0.15 in and $0.60 out per million tokens.
        assert!((ceiling.max_input - 0.15 * 0.02).abs() < 1e-12);
        assert!((ceiling.max_output - 0.60 * 0.02).abs() < 1e-12);
    }

    #[test]
    fn a_typo_tightens_the_ceiling_instead_of_disabling_it() {
        // A garbage value must not silently mean "no ceiling": that is the
        // exact configuration mistake that let the 49.8% tier through.
        for raw in ["abc", "-1", "NaN", "inf", ""] {
            let ceiling = PriceCeiling::parse(Some(raw), Some(raw));
            assert_eq!(ceiling.max_input, DEFAULT_MAX_INPUT_PRICE, "input {raw}");
            assert_eq!(ceiling.max_output, DEFAULT_MAX_OUTPUT_PRICE, "output {raw}");
        }
    }

    #[test]
    fn prices_are_rendered_without_float_noise() {
        assert_eq!(format_price(0.003), "0.003");
        assert_eq!(format_price(0.012), "0.012");
        assert_eq!(format_price(0.00015), "0.00015");
        assert_eq!(format_price(1.0), "1.0");
    }
}
