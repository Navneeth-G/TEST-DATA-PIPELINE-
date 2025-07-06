from pathlib import Path
# from data_pipeline_project.pipeline_logic.config_handler import main_config_handler
from pipeline_logic.config_handler import main_config_handler

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent  # This resolves to the 'v3' folder
    config = main_config_handler("projects/category/sub_category/config.json", str(project_root))

    from pprint import pprint
    pprint(config)
