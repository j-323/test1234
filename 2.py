import asyncio
from datetime import datetime
from typing import Dict, Optional
from uuid import UUID

TERMINAL = {"DONE", "ERROR"}

async def wait_dataset_files_status(
    uow,
    fms,                             
    dataset_uuid: UUID,
    *,
    main_files: Dict[UUID, UUID],        
    attachment_files: Dict[UUID, UUID],  
    interval_sec: int = 30,
    timeout_sec: Optional[int] = None,   
) -> Dict[UUID, str]:
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
        async with asyncio.timeout(timeout_sec):
            await _loop()

    now = datetime.now(getattr(settings, "DEFAULT_TIMEZONE", None))
    for fu, st in final_status.items():
        fu_str = str(fu)
        if fu_str in watch_main:
            await uow.document_repo.update_download_status(
                watch_main[fu_str],
                "success" if st == "DONE" else "error",
                now,
            )
        elif fu_str in watch_att:
            await uow.attachment_repo.update_download_status(
                watch_att[fu_str],
                "success" if st == "DONE" else "error",
                now,
            )

    await uow.commit()
    return final_status
