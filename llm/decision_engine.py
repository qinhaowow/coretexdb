from typing import Any, Callable, Dict, List, Optional, Tuple
import json
import numpy as np


class DecisionEngine:
    def __init__(
        self,
        llm_client=None,
        memory_manager=None,
        prompt_templates=None,
        confidence_threshold: float = 0.7,
        risk_threshold: float = 0.5
    ):
        self.llm_client = llm_client
        self.memory_manager = memory_manager
        self.prompt_templates = prompt_templates
        self.confidence_threshold = confidence_threshold
        self.risk_threshold = risk_threshold

        self.decision_history: List[Dict[str, Any]] = []
        self.action_validators: Dict[str, Callable] = {}
        self.safety_checks: List[Callable] = []

    def decide(
        self,
        situation: str,
        options: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        experiences = ""
        if self.memory_manager:
            memories = self.memory_manager.retrieve(situation, limit=5)
            experiences = "\n".join([str(m.get("content", "")) for m in memories])

        if self.llm_client and self.prompt_templates:
            options_str = json.dumps(options, indent=2)
            prompt = self.prompt_templates.get_decision_prompt(
                situation=situation,
                options=options_str,
                experiences=experiences
            )
            response = self.llm_client.generate(prompt, temperature=0.4)

            try:
                decision = json.loads(response)
            except json.JSONDecodeError:
                decision = self._fallback_decision(options, situation)
        else:
            decision = self._fallback_decision(options, situation)

        decision["situation"] = situation
        decision["timestamp"] = self._get_timestamp()

        self.decision_history.append(decision)

        return decision

    def _fallback_decide(
        self,
        options: List[Dict[str, Any]],
        situation: str
    ) -> Dict[str, Any]:
        if not options:
            return {
                "chosen_option": None,
                "confidence": 0.0,
                "reasoning": "No options provided",
                "risk_assessment": "Unknown"
            }

        scored_options = []
        for option in options:
            score = self._calculate_option_score(option, situation)
            scored_options.append((score, option))

        scored_options.sort(key=lambda x: x[0], reverse=True)
        best_score, best_option = scored_options[0]

        return {
            "chosen_option": best_option.get("id", best_option.get("name", "unknown")),
            "confidence": min(best_score, 1.0),
            "reasoning": f"Option selected based on heuristic scoring: {best_option.get('name', 'unknown')}",
            "risk_assessment": self._assess_risk(best_option),
            "alternative_if_failed": "retry" if best_score < self.confidence_threshold else "none"
        }

    def _calculate_option_score(self, option: Dict[str, Any], situation: str) -> float:
        score = 0.5

        if "priority" in option:
            score += option["priority"] * 0.2

        if "success_rate" in option:
            score += option["success_rate"] * 0.3

        if "cost" in option:
            cost_factor = max(0, 1 - option["cost"] / 100)
            score += cost_factor * 0.2

        if "estimated_time" in option:
            time_factor = max(0, 1 - option["estimated_time"] / 300)
            score += time_factor * 0.1

        if "required_capabilities" in option:
            capability_match = len(option.get("available_capabilities", [])) / max(len(option["required_capabilities"]), 1)
            score += capability_match * 0.2

        return score

    def _assess_risk(self, option: Dict[str, Any]) -> str:
        risk_level = "low"

        if "risk_factors" in option:
            if any(r.get("severity", 0) > 0.7 for r in option["risk_factors"]):
                risk_level = "high"
            elif any(r.get("severity", 0) > 0.4 for r in option["risk_factors"]):
                risk_level = "medium"

        return f"Risk level: {risk_level}"

    def validate_action(
        self,
        action: Dict[str, Any],
        current_state: Dict[str, Any]
    ) -> Tuple[bool, str]:
        action_type = action.get("type", "unknown")

        if action_type in self.action_validators:
            validator = self.action_validators[action_type]
            is_valid, message = validator(action, current_state)
            if not is_valid:
                return False, message

        for check in self.safety_checks:
            is_valid, message = check(action, current_state)
            if not is_valid:
                return False, message

        return True, "Action validated"

    def add_validator(self, action_type: str, validator: Callable):
        self.action_validators[action_type] = validator

    def add_safety_check(self, check: Callable):
        self.safety_checks.append(check)

    def get_decision_history(
        self,
        limit: int = 10,
        filter_by: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        history = self.decision_history[-limit:]

        if filter_by:
            filtered = []
            for decision in history:
                match = True
                for key, value in filter_by.items():
                    if decision.get(key) != value:
                        match = False
                        break
                if match:
                    filtered.append(decision)
            return filtered

        return history

    def get_statistics(self) -> Dict[str, Any]:
        if not self.decision_history:
            return {
                "total_decisions": 0,
                "average_confidence": 0.0,
                "high_confidence_rate": 0.0
            }

        confidences = [d.get("confidence", 0.0) for d in self.decision_history]
        high_conf_count = sum(1 for c in confidences if c >= self.confidence_threshold)

        return {
            "total_decisions": len(self.decision_history),
            "average_confidence": np.mean(confidences),
            "min_confidence": np.min(confidences),
            "max_confidence": np.max(confidences),
            "high_confidence_rate": high_conf_count / len(confidences),
            "unique_situations": len(set(d.get("situation", "") for d in self.decision_history))
        }

    def clear_history(self):
        self.decision_history.clear()

    def export_history(self, file_path: str) -> bool:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.decision_history, f, indent=2, ensure_ascii=False)
            return True
        except Exception:
            return False

    def _get_timestamp(self) -> str:
        from datetime import datetime
        return datetime.now().isoformat()
