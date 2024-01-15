import re
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.security import APIKeyHeader
from google.api_core.exceptions import NotFound
from google.cloud import firestore
from pydantic import BaseModel

# used to authenticate access to restricted parts of this api
key_header = APIKeyHeader(name="x-ayrj-key", auto_error=False)
KEY = "CENSORED"  # protect at all cost. if this leaks, we have lost everything

# regex to match doi urls so that they can be automatically extracted
# this is not foul-proof, there is no way to check if the matched url is valid without actually requesting doi.org
MATCH_DOI = re.compile("10\\.[\\.0-9]+/\\S*\\w")

db = firestore.AsyncClient(project="CENSORED")

# use the same collection id so that all papers can be searched with a collection group query
papers = db.collection("papers")

reviewing = papers.document("reviewing").collection("paper-data")
reviewing: firestore.AsyncCollectionReference

published = papers.document("published").collection("paper-data")
published: firestore.AsyncCollectionReference

retracted = papers.document("retracted").collection("paper-data")
retracted: firestore.AsyncCollectionReference

document_repo = db.collection("documents")


def format_id_to_string(id: int) -> str:
    """Returns string of `id` padded with `0`s on the left until 9 digits long and then split every 3 digits with a `-`"""

    # can be used for doi suffix too
    out = f"{id:09}"
    return "-".join([out[0:3], out[3:6], out[6:9]])


def generate_author_shorthand(authors: list[str]) -> str:
    """Return standard apa-like naming depending on number of authors"""

    match len(authors):
        case 0:
            raise ValueError("No authors provided")
        case 1:
            return authors[0]
        case 2:
            return " & ".join(authors[:2])
        case _:
            return f"{authors[0]} et al"


async def generate_unique_document_id(seed: bytes) -> str:
    """Generate a unique id by hashing the document contents and linear probing to avoid collisions"""

    # generate (hopefully) unique id
    id = hash(seed) % 1_000_000_000
    code = format_id_to_string(id)

    # check if the id is already used, increment it until it is no longer in use
    while (await document_repo.document(code).get()).exists:
        id = hash(code)
        id %= 1_000_000_000
        code = format_id_to_string(id)

    return code


@firestore.async_transactional
async def move_document(
    transaction: firestore.AsyncTransaction,
    document_id: str,
    from_collection: firestore.AsyncCollectionReference,
    to_collection: firestore.AsyncCollectionReference,
) -> dict[str, Any]:
    doc_ref = from_collection.document(document_id)
    document = await doc_ref.get(transaction=transaction)
    if not document.exists:
        raise ValueError(
            f"Document with id `{document_id}` does not exist in the specified collection"
        )

    doc_dict = document.to_dict()
    transaction.set(to_collection.document(document_id), doc_dict)
    transaction.delete(doc_ref)

    return doc_dict


class Correction(BaseModel):
    id: str

    date: datetime
    description: str


class Paper(BaseModel):
    id: str

    title: str
    abstract: str
    authors: list[str]
    category: str

    references: list[str]
    cited_by: list[str] = []

    submitted: datetime
    reviewed: list[datetime] = []
    published: datetime | None = None
    corrected: list[Correction] = []
    retracted: datetime | None = None


class PaperFile(BaseModel):
    name: str
    mime_type: str

    data: bytes


app = FastAPI()


@app.get("/")
async def welcome() -> str:
    return "Welcome to the AYRJ backend API"


@app.post("/submit")
async def submit(
    title: str = Form(),
    abstract: str = Form(),
    authors: list[str] = Form(),
    category: str = Form(),
    references: list[str] = Form(),
    doc: UploadFile = File(),
    key: str = Depends(key_header),
) -> str:
    """Handles initialising all paper data and adding it to the `reviewing` collection"""

    if key != KEY:
        raise HTTPException(401)

    # get file extension and check if its a valid file type
    match doc.content_type:
        case "application/msword":
            extension = "doc"
        case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            extension = "docx"
        case _:
            raise HTTPException(400, "Upload `.doc` or `.docx` files only")

    # shorthand for authors, also validates that `authors` is non-empty
    try:
        shorthand = generate_author_shorthand(authors)
    except ValueError:
        raise HTTPException(400, "No authors provided")

    doc_bytes = await doc.read(-1)
    await doc.close()

    code = await generate_unique_document_id(doc_bytes)

    write_batch = db.batch()

    # upload document
    write_batch.set(
        document_repo.document(code),
        PaperFile(
            name=f"{shorthand} DRAFT.{extension}",
            mime_type=doc.content_type,
            data=doc_bytes,
        ).model_dump(),
    )

    # create the `Paper` object, using pydantic parser to enforce type checking
    write_batch.set(
        reviewing.document(code),
        Paper(
            id=code,
            title=title,
            abstract=abstract,
            authors=authors,
            category=category,
            references=references,
            submitted=datetime.now(tz=timezone.utc),
        ).model_dump(),
    )

    await write_batch.commit()
    return code


@app.put("/publish")
async def publish(id: str, key: str = Depends(key_header)) -> None:
    """Moves paper from `reviewing` to `published` collection"""

    if key != KEY:
        raise HTTPException(401)

    doc_type = await document_repo.document(id).get(["mime_type"])

    try:
        if doc_type.to_dict()["mime_type"] != "application/pdf":
            raise HTTPException(400, "Change paper document to pdf before publication")

        await reviewing.document(id).update(
            {"published": datetime.now(tz=timezone.utc)}
        )
    except (NotFound, KeyError):
        raise HTTPException(
            400, f"Document with id {id} does not exist in `reviewing` collection"
        )

    await move_document(db.transaction(), id, reviewing, published)


@app.delete("/reject")
async def reject(id: str, key: str = Depends(key_header)) -> None:
    """Removes paper from `paper-data` collection and documents from `documents` colelction"""

    if key != KEY:
        raise HTTPException(401)

    delete_batch = db.batch()

    delete_batch.delete(reviewing.document(id))
    delete_batch.delete(document_repo.document(id))
    await delete_batch.commit()


@app.patch("/review")
async def review(
    id: str = Form(),
    title: str = Form(None),
    abstract: str = Form(None),
    authors: list[str] = Form(None),
    category: str = Form(None),
    references: list[str] = Form(None),
    doc: UploadFile = File(None),
    key: str = Depends(key_header),
) -> None:
    """Updates paper in `reviewing` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = reviewing.document(id)
    paper = await paper_ref.get(["authors"])

    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in `reviewing` collection")

    if doc is not None:
        match doc.content_type:
            case "application/msword":
                extension = "doc"
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                extension = "docx"
            case "application/pdf":
                extension = "pdf"
            case _:
                raise HTTPException(400, "Upload `.pdf`, `.doc` or `.docx` files only")

        await document_repo.document(id).set(
            PaperFile(
                name=f"{generate_author_shorthand(paper.to_dict()["authors"])} DRAFT.{extension}",
                mime_type=doc.content_type,
                data=await doc.read(-1),
            ).model_dump()
        )
        await doc.close()

    update_dict = {}

    if title is not None:
        update_dict |= {"title": title}
    if abstract is not None:
        update_dict |= {"abstract": abstract}
    if authors is not None:
        update_dict |= {"authors": authors}
    if category is not None:
        update_dict |= {"category": category}
    if references is not None:
        update_dict |= {"references": references}

    if update_dict or doc:
        await paper_ref.update(
            update_dict
            | {"reviewed": firestore.ArrayUnion([datetime.now(tz=timezone.utc)])}
        )


@app.put("/retract")
async def retract(id: str, key: str = Depends(key_header)) -> None:
    """Moves paper from `published` collection to `retracted` collection
    Retracts paper but does not actually delete any data"""

    if key != KEY:
        raise HTTPException(401)

    try:
        await published.document(id).update(
            {"retracted": datetime.now(tz=timezone.utc)}
        )
    except NotFound:
        raise HTTPException(
            400, f"Document with id `{id}` does not exist in `published` collection"
        )

    await move_document(db.transaction(), id, published, retracted)


@app.delete("/remove")
async def remove(id: str, key: str = Depends(key_header)) -> None:
    """Removes retracted paper data from `retracted` and `documents` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = retracted.document(id)
    paper = await paper_ref.get(["corrected"])
    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in retracted list")
    delete_batch = db.batch()
    for correction in paper.to_dict()["corrected"]:
        # remove all document data but leave the document name, this will prevent retracted `id`s from being reused
        delete_batch.set(document_repo.document(correction["id"]), {})

    delete_batch.delete(paper_ref)
    # remove all document data but leave the document name, this will prevent retracted `id`s from being reused
    delete_batch.set(document_repo.document(id), {})

    await delete_batch.commit()


@app.post("/correct")
async def correct(
    id: str = Form(),
    description: str = Form(),
    doc: UploadFile = File(),
    key: str = Depends(key_header),
) -> str:
    "Adds a correction to a published paper"

    if key != KEY:
        raise HTTPException(401)

    if doc.content_type != "application/pdf":
        raise HTTPException(400, "Please upload only `.pdf` files")

    paper_ref = published.document(id)
    paper = await paper_ref.get(["authors", "corrected"])
    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in published list")

    doc_bytes = await doc.read(-1)
    await doc.close()
    code = await generate_unique_document_id(doc_bytes)

    paper_dict = paper.to_dict()
    write_batch = db.batch()
    write_batch.set(
        document_repo.document(code),
        PaperFile(
            name=f"{generate_author_shorthand(paper_dict["authors"])} Correction {len(paper_dict["corrected"])+1}.pdf",
            mime_type=doc.content_type,
            data=doc_bytes,
        ).model_dump(),
    )

    write_batch.update(
        paper_ref,
        {
            "corrected": firestore.ArrayUnion(
                [
                    Correction(
                        id=code,
                        date=datetime.now(tz=timezone.utc),
                        description=description,
                    ).model_dump()
                ]
            )
        },
    )
    await write_batch.commit()
    return code
