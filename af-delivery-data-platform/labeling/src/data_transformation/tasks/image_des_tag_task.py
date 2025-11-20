import traceback

from constants import MediaType, TaskKey
from llm_orchestrator.image_des_tag import ImageDescriptionTagProcessor
from llm_orchestrator.llm_helpers import init_chat_model
from prompt_profiles_config import PromptTableType, PTImageDesTag
from task_framework import ProcessingResult, TaskSpec


def image_des_tag_processor(items: list[dict]) -> list[ProcessingResult]:
    source_files = [item["source_file"] for item in items]
    prompt_jsons = [item["prompt_json"] for item in items]

    assert len(set(prompt_jsons)) == 1, "All prompt_jsons must be the same to be batch processed"
    prompt_json = prompt_jsons[0]
    prompt = PTImageDesTag.model_validate_json(prompt_json)

    llms = []
    for model in prompt.models:
        llms.append(init_chat_model(model))

    processor = ImageDescriptionTagProcessor(
        image_desription_prompt=prompt.description_prompt_template.format(),
        image_tag_prompt=prompt.tag_prompt_template.format(),
        llms=llms,
    )

    results: list[ProcessingResult] = []
    try:
        batch_results = processor.batch(source_files)
        for r in batch_results:
            results.append(ProcessingResult(status="processed", result_json=r.model_dump_json(), error_message=None))
    except Exception as e:
        tb_str = traceback.format_exc()
        shared_error = f"{e!s}\ntraceback:\n{tb_str[:1000]}"
        for _ in range(len(source_files)):
            results.append(ProcessingResult(status="failed", result_json=None, error_message=shared_error))

    return results


def get_image_des_tag_spec() -> TaskSpec:
    return TaskSpec(
        task_key=TaskKey.image_des_tag,
        media_type=MediaType.image,
        prompt_table_type=PromptTableType.image_description_tag,
        processor=image_des_tag_processor,
        max_attempts=3,
    )
