from typing import Any, Dict, List, Optional
import json
import requests
from urllib.parse import urljoin


class OllamaClient:
    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama2",
        timeout: int = 120
    ):
        self.base_url = base_url
        self.model = model
        self.timeout = timeout

    def generate(
        self,
        prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 512,
        stream: bool = False,
        **kwargs
    ) -> str:
        url = urljoin(self.base_url, "/api/generate")

        payload = {
            "model": self.model,
            "prompt": prompt,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": stream
        }
        payload.update(kwargs)

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            return result.get("response", "")
        except requests.exceptions.RequestException as e:
            return f"Error: {str(e)}"

    def generate_stream(self, prompt: str, temperature: float = 0.7, max_tokens: int = 512):
        url = urljoin(self.base_url, "/api/generate")

        payload = {
            "model": self.model,
            "prompt": prompt,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout, stream=True)
            response.raise_for_status()
            for line in response.iter_lines():
                if line:
                    data = json.loads(line)
                    if "response" in data:
                        yield data["response"]
        except requests.exceptions.RequestException as e:
            yield f"Error: {str(e)}"

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 512
    ) -> str:
        url = urljoin(self.base_url, "/api/chat")

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            return result.get("message", {}).get("content", "")
        except requests.exceptions.RequestException as e:
            return f"Error: {str(e)}"

    def list_models(self) -> List[str]:
        url = urljoin(self.base_url, "/api/tags")

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            return [model.get("name", "") for model in result.get("models", [])]
        except requests.exceptions.RequestException as e:
            return []

    def check_connection(self) -> bool:
        try:
            response = requests.get(urljoin(self.base_url, "/api/tags"), timeout=5)
            return response.status_code == 200
        except:
            return False


class TaskPlanner:
    def __init__(
        self,
        llm_client: Optional[Any] = None,
        prompt_templates: Optional[Any] = None,
        max_replan_attempts: int = 3
    ):
        self.llm_client = llm_client or OllamaClient()
        self.prompt_templates = prompt_templates
        self.max_replan_attempts = max_replan_attempts

    def plan(self, goal: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context_str = json.dumps(context) if context else "{}"

        if self.prompt_templates:
            prompt = self.prompt_templates.get_planning_prompt(goal, context_str)
        else:
            prompt = f"""You are a robot task planner. Given the following goal and context, create a step-by-step plan.

Goal: {goal}

Context: {context_str}

Create a detailed plan with the following JSON format:
{{
    "task_name": "name of the task",
    "steps": [
        {{"step": 1, "action": "description of action", "expected_outcome": "what should happen"}}
    ],
    "estimated_duration": "estimated time in seconds",
    "required_capabilities": ["list of required robot capabilities"]
}}

Provide only the JSON output:"""

        response = self.llm_client.generate(prompt, temperature=0.3)

        try:
            plan = json.loads(response)
            return plan
        except json.JSONDecodeError:
            return {
                "task_name": "unknown",
                "steps": [{"step": 1, "action": response, "expected_outcome": ""}],
                "estimated_duration": "unknown",
                "required_capabilities": []
            }

    def replan(
        self,
        original_plan: Dict[str, Any],
        feedback: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        context_str = json.dumps(context) if context else "{}"
        plan_str = json.dumps(original_plan)

        if self.prompt_templates:
            prompt = self.prompt_templates.get_replanning_prompt(plan_str, feedback, context_str)
        else:
            prompt = f"""You are a robot task planner. The original plan failed or needs adjustment.

Original Plan:
{plan_str}

Feedback:
{feedback}

Context: {context_str}

Create a revised plan with the same JSON format:
{{
    "task_name": "name of the task",
    "steps": [
        {{"step": 1, "action": "description of action", "expected_outcome": "what should happen"}}
    ],
    "estimated_duration": "estimated time in seconds",
    "required_capabilities": ["list of required robot capabilities"]
}}

Provide only the JSON output:"""

        response = self.llm_client.generate(prompt, temperature=0.4)

        try:
            plan = json.loads(response)
            return plan
        except json.JSONDecodeError:
            return original_plan

    def decompose_task(
        self,
        task: str,
        max_subtasks: int = 5
    ) -> List[str]:
        if self.prompt_templates:
            prompt = self.prompt_templates.get_decomposition_prompt(task, max_subtasks)
        else:
            prompt = f"""Decompose the following task into {max_subtasks} or fewer subtasks.

Task: {task}

Provide the subtasks as a JSON array:
["subtask 1", "subtask 2", ...]"""

        response = self.llm_client.generate(prompt, temperature=0.3)

        try:
            subtasks = json.loads(response)
            return subtasks if isinstance(subtasks, list) else [task]
        except json.JSONDecodeError:
            return [task]

    def validate_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        is_valid = True
        errors = []

        if "task_name" not in plan:
            is_valid = False
            errors.append("Missing 'task_name' field")

        if "steps" not in plan or not isinstance(plan["steps"], list):
            is_valid = False
            errors.append("Missing or invalid 'steps' field")
        else:
            for i, step in enumerate(plan["steps"]):
                if not isinstance(step, dict):
                    is_valid = False
                    errors.append(f"Step {i} is not a dictionary")
                elif "action" not in step:
                    is_valid = False
                    errors.append(f"Step {i} missing 'action' field")

        return {
            "is_valid": is_valid,
            "errors": errors,
            "warnings": []
        }

    def estimate_duration(self, plan: Dict[str, Any]) -> float:
        if "steps" in plan:
            return len(plan["steps"]) * 30.0
        return 60.0
