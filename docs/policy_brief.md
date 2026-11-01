# Policy Brief: Seeing Trouble Earlier, District by District

**EpiScope — Bangladesh Disease Surveillance and Response Prototype**
**Prepared for:** the Director, Ministry of Health
**Group 1 — TriScope**, CSE4345 Big Data Analytics, Premier University Chittagong
**Date:** 02-11-2026

*This brief is written for readers without a technical background. It mirrors
Appendix A of the CEP technical report.*

## The Current Situation

Bangladesh fights dengue, diarrhoeal disease, influenza-like illness, and
malaria every year, but the information needed to act arrives late, in pieces,
and in different systems. By the time a rise in cases is obvious, the best
moment to pre-position supplies has often passed. District officers need an
early, simple, trustworthy answer to three questions: *Where is something
unusual happening? What should we expect next month? Where should limited
supplies go first?*

EpiScope is a working prototype that answers these questions for all 64
districts on a single screen. It was built and tested on 6.0 million realistic
(synthetic) patient-visit records over five years, so every number, alert, and
recommendation can be checked and reproduced before the system is ever trusted
with real data.

## Top 3 Recommendations and Expected Impact

1. **Review the early-warning list every week, district by district.** EpiScope
   flags unusual rises in cases and shows them in plain language ("highly
   unusual week"). In testing it caught 26 of 30 simulated outbreaks.
   *Expected impact: investigation teams start looking days earlier, while
   prevention is still cheap.*

2. **Pre-position supplies before the seasonal peak, not after it.** The
   seasonal calendar shows, for each district and disease, which weeks of the
   year are historically quiet, rising, or at peak, and what action is
   recommended. *Expected impact: fewer emergency deliveries and fewer
   districts caught without stock at the worst moment.*

3. **Treat the supply plan as a starting point for discussion, not an order.**
   The allocation tool recommends how to divide a fixed national budget across
   districts and explains its reasoning. In testing it met slightly more total
   need than a population-based share, but it concentrates supplies in a few
   districts — so district realities must be checked before deployment.
   *Expected impact: faster, more transparent allocation meetings with fewer
   disputes.*

## How to Read the Dashboard

The dashboard opens on the **whole country**. Pick a disease at the top; the
map shades each district by its recent case count (darker means more cases;
districts with very small counts are marked "small counts hidden" to protect
privacy). Click any district and every number on the page updates for that
district: cases this week, alerts raised, the forecast for next week with a
likely range, and how well supplies cover the need. The **Trends** page shows
the last year week by week, with the forecast drawn as a dashed line and a
shaded likely range. The **Supplies** page shows what is needed, what is sent,
and which districts are still short. The **Alerts** page lists recent unusual
weeks, newest first, and can be searched by district name. Everything works on
a phone as well as a computer.

## Confidence and Limitations

Be honest about what this prototype is. It has *not* been validated on real
patient data, so its alerts and forecasts show *where to look*, not *what is
true*. In testing, some signals arrived about five days after an outbreak had
already started, and most weekly alerts turned out not to be outbreaks at
all — which is why a human district review must confirm every signal before any
public action. Forecasts are planning estimates with a stated range, not
guarantees. Used this way — as a weekly discussion tool — the prototype is
ready for a supervised pilot with real data.

## Data-Protection Assurance

No names, phone numbers, or addresses appear anywhere in the system. Patient
identifiers are replaced with an unreadable code before any analysis begins,
and the secret key that could reverse it is kept outside the project files.
Counts of one to four cases are never shown, so no individual can be identified
from a published number. The public dashboard contains only district-level
weekly totals — there is no way to ask it about a person. These are the same
safeguards a real deployment would require, and they are open to audit in the
project repository.

## Bottom Line

EpiScope turns scattered routine reports into one clear weekly picture: where
cases are unusual, what is likely next, and how to divide limited supplies
fairly. It is a decision-support prototype, not an automatic alarm — its value
is an earlier, transparent, auditable conversation about where to investigate
and prepare first.

---

**Repository:** https://github.com/Md-Monir-Hossain-1165/episcope
**Dashboard:** https://episcope-m.vercel.app
