from typing import Any, Dict, List, Optional


class PromptTemplates:
    def __init__(self, template_dir: Optional[str] = None):
        self.template_dir = template_dir
        self._templates = self._load_default_templates()

    def _load_default_templates(self) -> Dict[str, str]:
        return {
            "system_prompt": """You are an intelligent robot memory system assistant. You help robots remember, reason about, and make decisions based on their past experiences and current context.

Your capabilities include:
- Answering questions about robot's past experiences
- Helping with task planning and reasoning
- Explaining robot behavior based on stored memories
- Assisting with decision making in novel situations""",

            "planning_prompt": """You are a robot task planner. Given the following goal and context, create a step-by-step plan.

Goal: {goal}

Context: {context}

Create a detailed plan with the following JSON format:
{{
    "task_name": "name of the task",
    "steps": [
        {{"step": 1, "action": "description of action", "expected_outcome": "what should happen", "dependencies": []}}
    ],
    "estimated_duration": "estimated time in seconds",
    "required_capabilities": ["list of required robot capabilities"],
    "fallback_strategies": ["alternative approaches if primary plan fails"]
}}

Provide only the JSON output:""",

            "replanning_prompt": """You are a robot task planner. The original plan failed or needs adjustment.

Original Plan:
{original_plan}

Feedback:
{feedback}

Context: {context}

Create a revised plan with the same JSON format:
{{
    "task_name": "name of the task",
    "steps": [
        {{"step": 1, "action": "description of action", "expected_outcome": "what should happen", "dependencies": []}}
    ],
    "estimated_duration": "estimated time in seconds",
    "required_capabilities": ["list of required robot capabilities"],
    "fallback_strategies": ["alternative approaches if primary plan fails"]
}}

Provide only the JSON output:""",

            "decomposition_prompt": """Decompose the following task into {max_subtasks} or fewer subtasks.

Task: {task}

Provide the subtasks as a JSON array:
["subtask 1", "subtask 2", ...]""",

            "memory_query_prompt": """Based on the robot's memory system, answer the following question.

Question: {query}

Relevant Memories:
{memories}

Context: {context}

Provide a detailed answer based on the robot's memories:""",

            "decision_prompt": """You are a robot decision engine. Given the current situation and available options, choose the best course of action.

Current Situation:
{situation}

Available Options:
{options}

Robot's Past Experiences:
{experiences}

Choose the best option and explain your reasoning in the following JSON format:
{{
    "chosen_option": "option identifier",
    "confidence": 0.0-1.0,
    "reasoning": "explanation of why this option was chosen",
    "risk_assessment": "potential risks and mitigations",
    "alternative_if_failed": "backup plan"
}}

Provide only the JSON output:""",

            "explanation_prompt": """Explain the robot's behavior based on its memory and current context.

Behavior to Explain:
{behavior}

Recent Memories:
{memories}

Current Context:
{context}

Provide a clear explanation of why the robot behaved this way:""",

            "conversation_prompt": """You are a robot with a memory system. You can recall past experiences and use them to inform your responses.

Current Conversation:
{conversation}

Relevant Past Experiences:
{experiences}

Robot's Knowledge Base:
{knowledge}

Respond naturally as the robot would, incorporating relevant memories:"""
        }

    def get_system_prompt(self) -> str:
        return self._templates["system_prompt"]

    def get_planning_prompt(self, goal: str, context: str = "{}") -> str:
        return self._templates["planning_prompt"].format(
            goal=goal,
            context=context
        )

    def get_replanning_prompt(
        self,
        original_plan: str,
        feedback: str,
        context: str = "{}"
    ) -> str:
        return self._templates["replanning_prompt"].format(
            original_plan=original_plan,
            feedback=feedback,
            context=context
        )

    def get_decomposition_prompt(self, task: str, max_subtasks: int = 5) -> str:
        return self._templates["decomposition_prompt"].format(
            task=task,
            max_subtasks=max_subtasks
        )

    def get_memory_query_prompt(
        self,
        query: str,
        memories: str,
        context: str = "{}"
    ) -> str:
        return self._templates["memory_query_prompt"].format(
            query=query,
            memories=memories,
            context=context
        )

    def get_decision_prompt(
        self,
        situation: str,
        options: str,
        experiences: str
    ) -> str:
        return self._templates["decision_prompt"].format(
            situation=situation,
            options=options,
            experiences=experiences
        )

    def get_explanation_prompt(
        self,
        behavior: str,
        memories: str,
        context: str = "{}"
    ) -> str:
        return self._templates["explanation_prompt"].format(
            behavior=behavior,
            memories=memories,
            context=context
        )

    def get_conversation_prompt(
        self,
        conversation: str,
        experiences: str,
        knowledge: str = ""
    ) -> str:
        return self._templates["conversation_prompt"].format(
            conversation=conversation,
            experiences=experiences,
            knowledge=knowledge
        )

    def add_template(self, name: str, template: str):
        self._templates[name] = template

    def remove_template(self, name: str) -> bool:
        if name in self._templates:
            del self._templates[name]
            return True
        return False

    def get_template(self, name: str) -> Optional[str]:
        return self._templates.get(name)

    def list_templates(self) -> List[str]:
        return list(self._templates.keys())
