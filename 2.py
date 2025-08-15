import asyncio
from datetime import datetime
from typing import Dict, Optional
from uuid import UUID

TERMINAL = {"DONE", "ERROR"}

async def wait_dataset_files_status(
    uow,
    fms,                               # должен иметь get_dataset_files_info(dataset_uuid)
    dataset_uuid: UUID,
    *,
    main_files: Dict[UUID, UUID],        # file_uuid -> doc_uuid
    attachment_files: Dict[UUID, UUID],  # file_uuid -> att_uuid
    interval_sec: int = 30,
    timeout_sec: Optional[int] = None,   # None -> ждать бесконечно
) -> Dict[UUID, str]:
    # ключи — строки, т.к. из JSON приходят строки
    watch_main = {str(k): v for k, v in main_files.items()}
    watch_att  = {str(k): v for k, v in attachment_files.items()}
    watched = set(watch_main) | set(watch_att)
    if not watched:
        return {}

    final_status: Dict[UUID, str] = {}

    async def _loop() -> None:
        nonlocal final_status
        while True:
            resp = await fms.get_dataset_files_info(dataset_uuid)
            files = (resp or {}).get("files", [])
            for item in files:
                fu = str(item.get("file_uuid") or "").strip()
                st = str(item.get("enrichment_status") or "").upper()
                if fu and fu in watched and st in TERMINAL:
                    final_status[UUID(fu)] = st

            if len(final_status) == len(watched):
                break

            await asyncio.sleep(interval_sec)

    if timeout_sec is None:
        await _loop()
    else:
        # для Python < 3.11
        await asyncio.wait_for(_loop(), timeout=timeout_sec)

    # --- фиксация в БД: получаем модели по UUID и присваиваем поля ---
    now = datetime.now(getattr(settings, "DEFAULT_TIMEZONE", None))
    for fu, st in final_status.items():
        fu_str = str(fu)
        status_str = "success" if st == "DONE" else "error"

        if fu_str in watch_main:
            doc_uuid = watch_main[fu_str]
            doc = await uow.document_repo.get_by_uuid(doc_uuid)
            if doc is not None:
                doc.download_status = status_str
                # если у модели есть поле времени загрузки — раскомментируйте
                # doc.downloaded_at = now

        elif fu_str in watch_att:
            att_uuid = watch_att[fu_str]
            att = await uow.attachment_repo.get_by_uuid(att_uuid)
            if att is not None:
                att.download_status = status_str
                # если у модели есть поле времени загрузки — раскомментируйте
                # att.downloaded_at = now

    await uow.commit()
    return final_status
