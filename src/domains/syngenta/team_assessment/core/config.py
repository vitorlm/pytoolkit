import ast
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    def __init__(self):
        # Extração de dados
        self._col_members = os.getenv("COL_MEMBERS")
        self._row_members_start = int(os.getenv("ROW_MEMBERS_START")) - 1
        self._row_members_end = int(os.getenv("ROW_MEMBERS_END"))
        self._row_days = int(os.getenv("ROW_DAYS")) - 1
        self._epics_helper_codes = [
            task.strip().lower() for task in os.getenv("EPICS_HELPER_CODES", "").split(",")
        ]
        self._epic_bug = os.getenv("EPIC_BUG").strip().lower()
        self._epic_out = os.getenv("EPIC_OUT").strip().lower()
        self._epic_spillover = os.getenv("EPIC_SPILLOVER").strip().lower()
        self._col_epics_start = os.getenv("COL_EPICS_START")
        self._col_epics_end = os.getenv("COL_EPICS_END")
        self._col_epics_assignment_start = os.getenv("COL_EPICS_ASSIGNMENT_START")
        self._row_epics_assignment_start = int(os.getenv("ROW_EPICS_ASSIGNMENT_START")) - 1
        self._row_epics_assignment_end = int(os.getenv("ROW_EPICS_ASSIGNMENT_END"))
        self._row_planned_epics_assignment_start = (
            int(os.getenv("ROW_PLANNED_EPICS_ASSIGNMENT_START")) - 1
        )
        self._row_planned_epics_assignment_end = int(os.getenv("ROW_PLANNED_EPICS_ASSIGNMENT_END"))
        self._row_header_start = int(os.getenv("ROW_HEADER_START")) - 1
        self._row_header_end = int(os.getenv("ROW_HEADER_END"))
        self._row_epics_start = int(os.getenv("ROW_EPICS_START")) - 1
        self._row_epics_end = int(os.getenv("ROW_EPICS_END"))

        # Calculate column indices only once
        self._col_member_idx = ord(self._col_members.upper()) - ord("A")
        self._col_epics_start_idx = ord(self._col_epics_start.upper()) - ord("A")
        self._col_epics_end_idx = ord(self._col_epics_end.upper()) - ord("A")
        self._col_epics_assignment_start_idx = ord(self._col_epics_assignment_start.upper()) - ord(
            "A"
        )

        # Conexão Ollama
        self._ollama_host = os.getenv("OLLAMA_HOST")
        self._ollama_model = os.getenv("OLLAMA_MODEL")

        # Runtime Ollama
        self._ollama_num_ctx = int(os.getenv("OLLAMA_NUM_CTX"))
        self._ollama_temperature = float(os.getenv("OLLAMA_TEMPERATURE"))
        self._ollama_num_thread = int(os.getenv("OLLAMA_NUM_THREAD"))
        self._ollama_num_keep = int(os.getenv("OLLAMA_NUM_KEEP"))
        self._ollama_top_k = int(os.getenv("OLLAMA_TOP_K"))
        self._ollama_top_p = float(os.getenv("OLLAMA_TOP_P"))
        self._ollama_repeat_penalty = float(os.getenv("OLLAMA_REPEAT_PENALTY"))
        self._ollama_stop = os.getenv("OLLAMA_STOP").split(",")
        self._ollama_num_predict = int(os.getenv("OLLAMA_NUM_PREDICT"))
        self._ollama_seed = int(os.getenv("OLLAMA_SEED"))
        self._ollama_embedding_only = os.getenv("OLLAMA_EMBEDDING_ONLY").lower() == "true"
        self._ollama_low_vram = os.getenv("OLLAMA_LOW_VRAM").lower() == "true"
        self._ollama_presence_penalty = float(os.getenv("OLLAMA_PRESENCE_PENALTY"))
        self._ollama_frequency_penalty = float(os.getenv("OLLAMA_FREQUENCY_PENALTY"))

        # Avaliação
        self._criteria_weights = ast.literal_eval(os.getenv("CRITERIA_WEIGHTS"))
        self._outlier_threshold = float(os.getenv("OUTLIER_THRESHOLD"))
        self._percentile_q1 = int(os.getenv("PERCENTILE_Q1"))
        self._percentile_q3 = int(os.getenv("PERCENTILE_Q3"))

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
    def col_epics_start_idx(self):
        return self._col_epics_start_idx

    @property
    def col_epics_end_idx(self):
        return self._col_epics_end_idx

    @property
    def col_epics_assignment_start_idx(self):
        return self._col_epics_assignment_start_idx

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
    def epics_helper_codes(self):
        return self._epics_helper_codes

    @property
    def col_epics_start(self):
        return self._col_epics_start

    @property
    def col_epics_end(self):
        return self._col_epics_end

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

    @property
    def epic_bug(self):
        return self._epic_bug

    @property
    def epic_out(self):
        return self._epic_out

    @property
    def epic_spillover(self):
        return self._epic_spillover

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
