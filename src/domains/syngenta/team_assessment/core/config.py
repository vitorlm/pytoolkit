import ast
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        # Extração de dados
        self._col_members = os.getenv("COL_MEMBERS", "C")
        self._row_members_start = int(os.getenv("ROW_MEMBERS_START", "4")) - 1
        self._row_members_end = int(os.getenv("ROW_MEMBERS_END", "14"))
        self._row_days = int(os.getenv("ROW_DAYS", "38")) - 1
        self._issue_helper_codes = [issue.strip().lower() for issue in os.getenv("NON_EPIC_CODES", "").split(",")]

        # Load activity categories (9 types)
        self._category_absence = [c.strip() for c in os.getenv("ACTIVITY_CATEGORY_ABSENCE", "").split(",") if c.strip()]
        self._category_support = [c.strip() for c in os.getenv("ACTIVITY_CATEGORY_SUPPORT", "").split(",") if c.strip()]
        self._category_improvement = [
            c.strip() for c in os.getenv("ACTIVITY_CATEGORY_IMPROVEMENT", "").split(",") if c.strip()
        ]
        self._category_overdue = [c.strip() for c in os.getenv("ACTIVITY_CATEGORY_OVERDUE", "").split(",") if c.strip()]
        self._category_seconded = [
            c.strip() for c in os.getenv("ACTIVITY_CATEGORY_SECONDED", "").split(",") if c.strip()
        ]
        self._category_learning = [
            c.strip() for c in os.getenv("ACTIVITY_CATEGORY_LEARNING", "").split(",") if c.strip()
        ]
        self._category_planning = [
            c.strip() for c in os.getenv("ACTIVITY_CATEGORY_PLANNING", "").split(",") if c.strip()
        ]
        self._category_unplanned = [
            c.strip() for c in os.getenv("ACTIVITY_CATEGORY_UNPLANNED", "").split(",") if c.strip()
        ]

        # Create reverse mapping: code -> category
        self._code_to_category = {}
        for code in self._category_absence:
            self._code_to_category[code.lower()] = "ABSENCE"
        for code in self._category_support:
            self._code_to_category[code.lower()] = "SUPPORT"
        for code in self._category_improvement:
            self._code_to_category[code.lower()] = "IMPROVEMENT"
        for code in self._category_overdue:
            self._code_to_category[code.lower()] = "OVERDUE"
        for code in self._category_seconded:
            self._code_to_category[code.lower()] = "SECONDED"
        for code in self._category_learning:
            self._code_to_category[code.lower()] = "LEARNING"
        for code in self._category_planning:
            self._code_to_category[code.lower()] = "PLANNING"
        for code in self._category_unplanned:
            self._code_to_category[code.lower()] = "UNPLANNED"

        self._col_allocation_members = os.getenv("COL_ALLOCATION_MEMBERS", "J")
        self._col_epics_assignment_start = os.getenv("COL_EPICS_ASSIGNMENT_START", "K")
        self._row_epics_assignment_start = int(os.getenv("ROW_EPICS_ASSIGNMENT_START", "39")) - 1
        self._row_epics_assignment_end = int(os.getenv("ROW_EPICS_ASSIGNMENT_END", "48"))
        self._row_planned_epics_assignment_start = int(os.getenv("ROW_PLANNED_EPICS_ASSIGNMENT_START", "53")) - 1
        self._row_planned_epics_assignment_end = int(os.getenv("ROW_PLANNED_EPICS_ASSIGNMENT_END", "61"))
        self._row_header_start = int(os.getenv("ROW_HEADER_START", "3")) - 1
        self._row_header_end = int(os.getenv("ROW_HEADER_END", "5"))
        self._row_epics_start = int(os.getenv("ROW_EPICS_START", "6")) - 1
        self._row_epics_end = int(os.getenv("ROW_EPICS_END", "33"))

        # Epic headers (6 columns: priority, dev_type, code, jira, subject, type)
        self._col_priority = os.getenv("COL_PRIORITY", "H")
        self._col_dev_type = os.getenv("COL_DEV_TYPE", "I")
        self._col_code = os.getenv("COL_CODE", "J")
        self._col_jira = os.getenv("COL_JIRA", "K")
        self._col_subject = os.getenv("COL_SUBJECT", "L")
        self._col_type = os.getenv("COL_TYPE", "S")

        # Days/calendar configuration
        self._col_days_start = os.getenv("COL_DAYS_START", "K")
        self._cycle_c1_days = int(os.getenv("CYCLE_C1_DAYS", "30"))
        self._cycle_c2_days = int(os.getenv("CYCLE_C2_DAYS", "35"))

        # Calculate column indices only once
        self._col_member_idx = ord(self._col_members.upper()) - ord("A")
        self._col_allocation_members_idx = ord(self._col_allocation_members.upper()) - ord("A")
        self._col_epics_assignment_start_idx = ord(self._col_epics_assignment_start.upper()) - ord("A")
        self._col_priority_idx = ord(self._col_priority.upper()) - ord("A")
        self._col_dev_type_idx = ord(self._col_dev_type.upper()) - ord("A")
        self._col_code_idx = ord(self._col_code.upper()) - ord("A")
        self._col_jira_idx = ord(self._col_jira.upper()) - ord("A")
        self._col_subject_idx = ord(self._col_subject.upper()) - ord("A")
        self._col_type_idx = ord(self._col_type.upper()) - ord("A")
        self._col_days_start_idx = ord(self._col_days_start.upper()) - ord("A")

        # Conexão Ollama
        self._ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        self._ollama_model = os.getenv("OLLAMA_MODEL", "TalentForgeAI")

        # Runtime Ollama
        self._ollama_num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "16384"))
        self._ollama_temperature = float(os.getenv("OLLAMA_TEMPERATURE", "0.5"))
        self._ollama_num_thread = int(os.getenv("OLLAMA_NUM_THREAD", "8"))
        self._ollama_num_keep = int(os.getenv("OLLAMA_NUM_KEEP", "1024"))
        self._ollama_top_k = int(os.getenv("OLLAMA_TOP_K", "20"))
        self._ollama_top_p = float(os.getenv("OLLAMA_TOP_P", "0.8"))
        self._ollama_repeat_penalty = float(os.getenv("OLLAMA_REPEAT_PENALTY", "1.1"))
        self._ollama_stop = os.getenv("OLLAMA_STOP", "END").split(",")
        self._ollama_num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "4096"))
        self._ollama_seed = int(os.getenv("OLLAMA_SEED", "42"))
        self._ollama_embedding_only = os.getenv("OLLAMA_EMBEDDING_ONLY", "false").lower() == "true"
        self._ollama_low_vram = os.getenv("OLLAMA_LOW_VRAM", "false").lower() == "true"
        self._ollama_presence_penalty = float(os.getenv("OLLAMA_PRESENCE_PENALTY", "1"))
        self._ollama_frequency_penalty = float(os.getenv("OLLAMA_FREQUENCY_PENALTY", "0.5"))

        # Avaliação
        criteria_weights_str = os.getenv(
            "CRITERIA_WEIGHTS",
            '{"Technical Skills": 0.4, "Delivery Skills": 0.25, "Soft Skills": 0.25, "Values and Behaviors": 0.1}',
        )
        self._criteria_weights = ast.literal_eval(criteria_weights_str)
        self._outlier_threshold = float(os.getenv("OUTLIER_THRESHOLD", "1.5"))
        self._percentile_q1 = int(os.getenv("PERCENTILE_Q1", "25"))
        self._percentile_q3 = int(os.getenv("PERCENTILE_Q3", "75"))

    # --- Properties for data extraction ---
    @property
    def row_planned_epics_assignment_start(self):
        return self._row_planned_epics_assignment_start

    @property
    def row_planned_epics_assignment_end(self):
        return self._row_planned_epics_assignment_end

    @property
    def row_epics_assignment_start(self):
        return self._row_epics_assignment_start

    @property
    def row_epics_assignment_end(self):
        return self._row_epics_assignment_end

    @property
    def col_member_idx(self):
        return self._col_member_idx

    @property
    def col_members(self):
        return self._col_members

    @property
    def col_allocation_members_idx(self):
        return self._col_allocation_members_idx

    @property
    def col_epics_assignment_start_idx(self):
        return self._col_epics_assignment_start_idx

    @property
    def col_priority(self):
        return self._col_priority

    @property
    def col_priority_idx(self):
        return self._col_priority_idx

    @property
    def col_dev_type(self):
        return self._col_dev_type

    @property
    def col_dev_type_idx(self):
        return self._col_dev_type_idx

    @property
    def col_code(self):
        return self._col_code

    @property
    def col_code_idx(self):
        return self._col_code_idx

    @property
    def col_jira(self):
        return self._col_jira

    @property
    def col_jira_idx(self):
        return self._col_jira_idx

    @property
    def col_subject(self):
        return self._col_subject

    @property
    def col_subject_idx(self):
        return self._col_subject_idx

    @property
    def col_type(self):
        return self._col_type

    @property
    def col_type_idx(self):
        return self._col_type_idx

    @property
    def col_days_start(self):
        return self._col_days_start

    @property
    def col_days_start_idx(self):
        return self._col_days_start_idx

    @property
    def cycle_c1_days(self):
        return self._cycle_c1_days

    @property
    def cycle_c2_days(self):
        return self._cycle_c2_days

    @property
    def row_members_start(self):
        return self._row_members_start

    @property
    def row_members_end(self):
        return self._row_members_end

    @property
    def row_days(self):
        return self._row_days

    @property
    def issue_helper_codes(self):
        return self._issue_helper_codes

    @property
    def col_epics_assignment_start(self):
        return self._col_epics_assignment_start

    @property
    def row_epics_start(self):
        return self._row_epics_start

    @property
    def row_epics_end(self):
        return self._row_epics_end

    @property
    def row_header_start(self):
        return self._row_header_start

    @property
    def row_header_end(self):
        return self._row_header_end

    # --- Activity Category Methods ---
    def get_category_for_code(self, code: str) -> str:
        """Determine the activity category for a given task code.

        Args:
            code: Task code from Planning Excel (e.g., "Bug", "Spillover", "MyCropwise")

        Returns:
            Category name: ABSENCE, SUPPORT, IMPROVEMENT, OVERDUE, SECONDED,
                          LEARNING, PLANNING, UNPLANNED, or DEVELOPMENT (default)
        """
        code_lower = code.lower()
        return self._code_to_category.get(code_lower, "DEVELOPMENT")

    @property
    def category_absence(self):
        """List of codes classified as ABSENCE activities."""
        return self._category_absence

    @property
    def category_support(self):
        """List of codes classified as SUPPORT activities."""
        return self._category_support

    @property
    def category_improvement(self):
        """List of codes classified as IMPROVEMENT activities."""
        return self._category_improvement

    @property
    def category_overdue(self):
        """List of codes classified as OVERDUE activities."""
        return self._category_overdue

    @property
    def category_seconded(self):
        """List of codes classified as SECONDED activities."""
        return self._category_seconded

    @property
    def category_learning(self):
        """List of codes classified as LEARNING activities."""
        return self._category_learning

    @property
    def category_planning(self):
        """List of codes classified as PLANNING activities."""
        return self._category_planning

    @property
    def category_unplanned(self):
        """List of codes classified as UNPLANNED activities."""
        return self._category_unplanned

    # --- Properties for Ollama connection ---
    @property
    def ollama_host(self):
        return self._ollama_host

    @property
    def ollama_model(self):
        return self._ollama_model

    @property
    def ollama_num_ctx(self):
        return self._ollama_num_ctx

    @property
    def ollama_temperature(self):
        return self._ollama_temperature

    @property
    def ollama_num_thread(self):
        return self._ollama_num_thread

    @property
    def ollama_num_keep(self):
        return self._ollama_num_keep

    @property
    def ollama_top_k(self):
        return self._ollama_top_k

    @property
    def ollama_top_p(self):
        return self._ollama_top_p

    @property
    def ollama_repeat_penalty(self):
        return self._ollama_repeat_penalty

    @property
    def ollama_stop(self):
        return self._ollama_stop

    @property
    def ollama_num_predict(self):
        return self._ollama_num_predict

    @property
    def ollama_seed(self):
        return self._ollama_seed

    @property
    def ollama_embedding_only(self):
        return self._ollama_embedding_only

    @property
    def ollama_low_vram(self):
        return self._ollama_low_vram

    @property
    def ollama_presence_penalty(self):
        return self._ollama_presence_penalty

    @property
    def ollama_frequency_penalty(self):
        return self._ollama_frequency_penalty

    # --- Properties for evaluation ---
    @property
    def criteria_weights(self):
        return self._criteria_weights

    @property
    def outlier_threshold(self):
        return self._outlier_threshold

    @property
    def percentile_q1(self):
        return self._percentile_q1

    @property
    def percentile_q3(self):
        return self._percentile_q3

    def get_ollama_config(self):
        """Returns a dictionary with all Ollama configuration parameters."""
        return {
            "num_ctx": self.ollama_num_ctx,
            "temperature": self.ollama_temperature,
            "num_thread": self.ollama_num_thread,
            "num_keep": self.ollama_num_keep,
            "top_k": self.ollama_top_k,
            "top_p": self.ollama_top_p,
            "repeat_penalty": self.ollama_repeat_penalty,
            "stop": self.ollama_stop,
            "num_predict": self.ollama_num_predict,
            "seed": self.ollama_seed,
            "embedding_only": self.ollama_embedding_only,
            "low_vram": self.ollama_low_vram,
            "presence_penalty": self.ollama_presence_penalty,
            "frequency_penalty": self.ollama_frequency_penalty,
        }
