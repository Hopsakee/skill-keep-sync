# EvalSkill Workflow

Give a skill a durable, re-runnable **binary acceptance contract** (`Evals.md`) and grade output against it in a **fresh context** to defeat author bias.

This is the regression half of skill quality. It is a *different axis* from TestSkill: TestSkill answers "does this skill add value over no skill?" (a marginal-value signal); evals answer "does this skill's output still meet a fixed bar after I changed it?" (a regression signal). Run both — TestSkill when deciding whether a skill is worth having, EvalSkill on every subsequent edit.

## Voice Notification

```bash
curl -s -X POST http://localhost:31337/notify \
  -H "Content-Type: application/json" \
  -d '{"message": "Running the EvalSkill workflow in the CreateSkill skill to grade skill output"}' \
  > /dev/null 2>&1 &
```

Running the **EvalSkill** workflow in the **CreateSkill** skill to grade skill output...

---

## Why binary, not a 1–5 score

An LLM grader cannot reliably tell a 3/5 from a 4/5 — the boundary is noise, and the "score" drifts run-to-run, so it can't detect regression. A **binary pass/fail** check forces you to write down the *specific observable behavior* that must hold. Ten honest yes/no checks beat one fuzzy rubric every time. If you find yourself wanting a score, you haven't yet named the behavior — split it into the two or three binary checks hiding inside it.

## Step 1: Write `Evals.md`

Create `Evals.md` in the skill's root directory (a level-3 context file — flat, alongside SKILL.md). Group ~8–12 binary checks by category. Each check is a single observable claim a grader can mark PASS or FAIL by reading the output alone.

```markdown
# Evals — <SkillName>

> Binary acceptance contract. Re-run after every change to the skill.
> Each check is PASS/FAIL only. No scores. Accumulate checks as failures teach you new ones.

## <Category: e.g. Structure>
- [ ] The output includes <observable thing>.
- [ ] <Anti-check: the output does NOT include <thing that must never appear>>.

## <Category: e.g. Substance>
- [ ] <Observable claim about content quality, phrased yes/no>.

## <Category: e.g. Voice / format>
- [ ] <Observable claim>.
```

Rules for good checks:
- **Observable, not aspirational.** "Leads with the recommendation in the first sentence" (gradeable) beats "is well-structured" (not gradeable).
- **At least one anti-check per eval set** — a behavior that must NEVER appear (the regression you're guarding against).
- **Derive checks from real failures.** Every time output disappoints, add the check that would have caught it. The eval set is *accumulated failure knowledge made executable* — this is what makes it anti-fragile.
- A skill's `Evals.md` is private-or-public by the same rule as the skill: a `_ALLCAPS` skill's evals may name real specifics; a `TitleCase` skill's evals stay generic.

## Step 2: Produce output (producer agent)

Run the skill on a realistic prompt and capture the output to a file:

```
[workspace]/eval-<skillname>/output.md
```

The producer can be you in-session, or a subagent. What matters for Step 3 is that the *grader* does not share the producer's context.

## Step 3: Grade in a FRESH context (grader agent)

**The single most important rule: the agent that grades is NOT the agent that wrote or improved the skill.** An author silently grades their own homework with privileged knowledge of intent — they "see" quality that isn't on the page. A fresh grader sees only what a real user would: the output and the checks.

Spawn a grader subagent with a clean context window. Give it *only* the output and `Evals.md` — never the skill's reasoning, the chat history, or your intent:

```
You are grading output against a fixed acceptance contract. You have no other context.

Output to grade: [absolute path to output.md]
Acceptance contract: [absolute path to Evals.md]

For EACH check, mark PASS or FAIL based ONLY on what is present in the output.
If you are unsure, mark FAIL — the check was not unambiguously met.
Return: a table of check → PASS/FAIL → one-line evidence quoted from the output.
Then: total passed / total, and the single most important failure if any.
```

## Step 4: Iterate on fail (thin crutch, not infrastructure)

If checks fail, fix the skill (via `Workflows/ImproveSkill.md`), regenerate output, and re-grade in a fresh context. Repeat until all checks pass.

**Keep this loop thin.** The iterate-until-pass machinery exists only because a current model's first pass is unreliable; a stronger model passes sooner and the loop shrinks to nothing. Do not build elaborate iteration bookkeeping or a permanent harness around it. The *durable* asset is `Evals.md` + the fresh-context grade — the loop is just how you use them today. (BPE: keep the harness and the accumulated checks; treat the retry cascade as a crutch you'd happily delete.)

## Step 5: Record

- Append the eval result to the skill's execution log (status `ok` if all passed).
- If a failure revealed a non-obvious lesson, add the new check to `Evals.md` AND, if it's a failure-mode worth warning future invocations about, a line to the skill's `## Gotchas`.

## Relationship to other surfaces (no duplication)

| Surface | What it holds | Distinct from evals because… |
|---------|---------------|------------------------------|
| `Evals.md` (this workflow) | Binary, re-runnable acceptance checks | It's the regression contract — re-checked every edit |
| `## Gotchas` (SKILL.md) | Failure-mode knowledge, API quirks, "watch out for X" | It warns the *author/agent*; it isn't a pass/fail grade of *output* |
| TestSkill | With-skill vs baseline comparison | It measures marginal value, not conformance to a fixed bar |
| Evals **skill** (`skills/Evals/`) | Heavyweight project-level eval suites, multi-model, golden outputs | That's for shipping products; this is a lightweight dev-time gate in the skill itself |
| `execution.jsonl` | Operational telemetry (when/how-long/ok) | Telemetry, not quality |

**Subjective taste feedback** ("make the voice more authentic") that resists binarization does NOT get its own file — it lands in `## Gotchas` as a `Lessons:` note, or you find the binary check hiding inside it and add that to `Evals.md`. **AI-slop tells** (em-dash overuse, "X, not Y" antithesis, hollow closers) are not a separate cleanup pass — they're covered by your writing-style surface (`PAI/USER/AI_WRITING_PATTERNS.md`), and if you want them enforced here, encode one binary check ("no em-dash density above the Ai-writing-patterns threshold") in `Evals.md`.
