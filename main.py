import re
from datetime import datetime, timezone
from random import randint
from typing import Annotated, Any, Literal

import aiofiles
import aiofiles.os
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from google.api_core.exceptions import NotFound
from google.cloud import firestore
from pydantic import BaseModel, Field, NonNegativeInt
from thefuzz import fuzz

# used to authenticate access to restricted parts of this api
key_header = APIKeyHeader(name="x-ayrj-key", auto_error=False)
KEY = "In science we find truth, @ ayrj we have this API"  # protect at all cost. if this leaks, we have lost everything

# regex to match doi urls so that they can be automatically extracted
# this is not foul-proof, there is no way to check if the matched url is valid without actually requesting doi.org
MATCH_DOI = re.compile("10\\.[\\.0-9]+/\\S*\\w")

# file path for mounted gcloud storage FUSE
DOCS_PATH = "/ayrj-docs"

db = firestore.AsyncClient(project="ayrj-backend")

# use the same collection id so that all papers can be searched with a collection group query
papers = db.collection("papers")

reviewing = papers.document("reviewing").collection("paper-data")
reviewing: firestore.AsyncCollectionReference

published = papers.document("published").collection("paper-data")
published: firestore.AsyncCollectionReference

retracted = papers.document("retracted").collection("paper-data")
retracted: firestore.AsyncCollectionReference

featured = db.collection("featured")


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


async def generate_unique_document_id() -> str:
    """Generate a unique id by hashing the document contents and linear probing to avoid collisions"""

    # generate (hopefully) unique id
    id = randint(0, 999_999_999)
    code = format_id_to_string(id)

    # check if the id is already used, increment it until it is no longer in use
    while await aiofiles.os.path.exists(f"{DOCS_PATH}/papers/{code}"):
        id = randint(0, 999_999_999)
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

    document_name: str


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

    document_name: str
    document_mimetype: str


app = FastAPI()

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


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
            extension = ".doc"
        case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            extension = ".docx"
        case "application/pdf":
            extension = ".pdf"
        case _:
            raise HTTPException(
                415,
                f"Upload `.pdf`, `.doc` or `.docx` files only, not `{doc.content_type}`",
            )

    # shorthand for authors, also validates that `authors` is non-empty
    try:
        shorthand = generate_author_shorthand(authors)
    except ValueError:
        raise HTTPException(422, "No authors provided")

    code = await generate_unique_document_id()

    # save the document and close the temp file
    async with aiofiles.open(f"{DOCS_PATH}/papers/{code}", "wb") as file:
        await file.write(await doc.read(-1))
    await doc.close()

    # create the `Paper` object, using pydantic parser to enforce type checking
    # then upload it to firestore
    await reviewing.document(code).set(
        Paper(
            id=code,
            title=title,
            abstract=abstract,
            authors=authors,
            category=category,
            references=references,
            submitted=datetime.now(tz=timezone.utc),
            document_name=f"{shorthand} DRAFT{extension}",
            document_mimetype=doc.content_type,
        ).model_dump()
    )

    # return the id of the paper under review
    return code


@app.patch("/publish")
async def publish(id: str, key: str = Depends(key_header)) -> None:
    """Moves paper from `reviewing` to `published` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper = await reviewing.document(id).get(["document_mimetype", "authors"])

    if not paper.exists:
        raise HTTPException(
            404, f"Document with id {id} does not exist in `reviewing` collection"
        )

    paper_dict = paper.to_dict()

    if paper_dict["document_mimetype"] != "application/pdf":
        raise HTTPException(415, "Change paper document to pdf before publication")

    now = datetime.now(tz=timezone.utc)
    await reviewing.document(id).update(
        {
            "published": now,
            "document_name": f"{generate_author_shorthand(paper_dict['authors'])} ({now.strftime('%Y')}).pdf",
        }
    )

    await move_document(db.transaction(), id, reviewing, published)


@app.delete("/reject")
async def reject(id: str, key: str = Depends(key_header)) -> None:
    """Removes paper data from `paper-data` collection and documents from gcloud storage"""

    if key != KEY:
        raise HTTPException(401)

    await reviewing.document(id).delete()
    await aiofiles.os.remove(f"{DOCS_PATH}/papers/{id}")


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
    paper = await paper_ref.get(["authors", "title", "abstract", "references"])

    if not paper.exists:
        raise HTTPException(
            404, f"Document with id `{id}` does not exist in `reviewing` collection"
        )

    update_dict = {}

    paper_dict = paper.to_dict()

    if doc is not None:
        match doc.content_type:
            case "application/msword":
                extension = ".doc"
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                extension = ".docx"
            case "application/pdf":
                extension = ".pdf"
            case _:
                raise HTTPException(415, "Upload `.pdf`, `.doc` or `.docx` files only")

        async with aiofiles.open(f"{DOCS_PATH}/papers/{id}", "wb") as file:
            await file.write(await doc.read(-1))
        await doc.close()

        update_dict |= {
            "document_name": f"{generate_author_shorthand(paper_dict['authors'])} DRAFT.{extension}",
            "document_mimetype": doc.content_type,
        }

    if title:
        update_dict |= {"title": title}
    if abstract:
        update_dict |= {"abstract": abstract}
    if authors:
        update_dict |= {"authors": authors}
    if category:
        update_dict |= {"category": category}
    if references:
        update_dict |= {"references": references}

    if update_dict or doc:
        await paper_ref.update(
            update_dict
            | {
                "reviewed": firestore.ArrayUnion([datetime.now(tz=timezone.utc)]),
            }
        )


@app.patch("/retract")
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
            404, f"Document with id `{id}` does not exist in `published` collection"
        )

    await move_document(db.transaction(), id, published, retracted)


@app.delete("/remove")
async def remove(id: str, key: str = Depends(key_header)) -> None:
    """Removes retracted paper data from `retracted` collection and truncates files in google cloud storage"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = retracted.document(id)
    paper = await paper_ref.get(["corrected"])
    if not paper.exists:
        raise HTTPException(
            404, f"Document with id `{id}` does not exist in `retracted` collection"
        )

    await paper_ref.delete()

    for correction in paper.to_dict()["corrected"]:
        correction_id = correction["id"]
        # remove all document data but leave the document name, this will prevent retracted `id`s from being reused
        async with aiofiles.open(f"{DOCS_PATH}/papers/{correction_id}", "wb") as _:
            pass

    # remove all document data but leave the document name, this will prevent retracted `id`s from being reused
    async with aiofiles.open(f"{DOCS_PATH}/papers/{id}", "wb") as _:
        pass


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
        raise HTTPException(415, "Please upload only `.pdf` files")

    paper_ref = published.document(id)
    paper = await paper_ref.get(["authors", "corrected", "published"])
    if not paper.exists:
        raise HTTPException(
            404, f"Document with id `{id}` does not exist in `published` collection"
        )

    code = await generate_unique_document_id()
    paper_dict = paper.to_dict()

    async with aiofiles.open(f"{DOCS_PATH}/papers/{code}", "wb") as file:
        await file.write(await doc.read(-1))
    await doc.close()

    await paper_ref.update(
        {
            "corrected": firestore.ArrayUnion(
                [
                    Correction(
                        id=code,
                        date=datetime.now(tz=timezone.utc),
                        description=description,
                        document_name=f"{generate_author_shorthand(paper_dict['authors'])} ({paper_dict['published'].strftime('%Y')}) Correction {len(paper_dict['corrected'])+1}.pdf",
                    ).model_dump()
                ]
            ),
        },
    )

    return code


@app.get("/list/{paper_type}")
async def list_papers(
    paper_type: Literal["published", "reviewing", "retracted", "all"],
    length: NonNegativeInt = 1,
    start_at_id: str | None = None,
    start_at_date: datetime | None = None,
    end_before_date: datetime | None = None,
    category: str | None = None,
    contains: str = "",
    quality_limit: Annotated[int, Field(ge=0, le=100)] = 100,
    key: str = Depends(key_header),
) -> list[Paper]:
    match paper_type:
        case "published":
            listing = published
            date = "published"
        case "retracted":
            listing = retracted
            date = "retracted"
        case "reviewing":
            if key != KEY:
                raise HTTPException(401)
            listing = reviewing
            date = "submitted"
        case "all":
            if key != KEY:
                raise HTTPException(401)
            listing = db.collection_group("paper-data")
            date = "submitted"
    listing = listing.order_by("id").order_by(date)
    if start_at_id is not None:
        listing = listing.start_at({"id": start_at_id})

    if category is not None:
        listing = listing.where(
            filter=firestore.FieldFilter("category", "==", category)
        )

    if start_at_date is not None:
        listing = listing.where(filter=firestore.FieldFilter(date, ">=", start_at_date))

    if end_before_date is not None:
        listing = listing.where(
            filter=firestore.FieldFilter(date, "<", end_before_date)
        )

    out = []
    out_len = 0

    if contains == "":
        quality_limit = 0

    async for paper in listing.stream():
        paper = Paper.model_validate(paper.to_dict())

        if (
            fuzz.partial_ratio(contains, paper.title) >= quality_limit
            or fuzz.partial_ratio(contains, paper.abstract) >= quality_limit
            or fuzz.partial_ratio(contains, paper.references) >= quality_limit
            or any(
                fuzz.partial_ratio(author, contains) >= quality_limit
                for author in paper.authors
            )
        ):
            out.append(paper)
            out_len += 1
            if out_len >= length:
                break

    return out


@app.get("/get/{paper_type}")
async def get_paper(
    paper_type: Literal["published", "reviewing", "journal", "template", "form"],
    id: str,
    key: str = Depends(key_header),
) -> FileResponse:
    match paper_type:
        case "published":
            collection = published
        case "reviewing":
            if key != KEY:
                raise HTTPException(401)
            collection = reviewing
        case "journal":
            path = f"{DOCS_PATH}/journals/{id}"
            if not (await aiofiles.os.path.isfile(path)):
                raise HTTPException(
                    404, "A publication with that id could not be found"
                )
            return FileResponse(path, media_type="application/pdf", filename=id)
        case "template":
            return FileResponse(
                f"{DOCS_PATH}/template",
                media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                filename="AYRJ Manuscript Template",
            )
        case "form":
            return FileResponse(
                f"{DOCS_PATH}/form",
                media_type="application/pdf",
                filename="AYRJ Application Form",
            )

    paper = await collection.document(id).get(["document_name", "document_mimetype"])
    if not paper.exists:
        raise HTTPException(
            404,
            f"Paper with `id`: {id} does not exist in `{paper_type}` collection",
        )

    paper_dict = paper.to_dict()
    return FileResponse(
        f"{DOCS_PATH}/papers/{id}",
        media_type=paper_dict["document_mimetype"],
        filename=paper_dict["document_name"],
    )


@app.get("/num-papers")
async def count_papers() -> int:
    total = 0

    async for _ in published.select([]).stream():
        total += 1

    return total


@app.put("/feature")
async def feature(id: str, key: str = Depends(key_header)) -> None:
    if key != KEY:
        raise HTTPException(401)

    if not (await published.document(id).get([])).exists:
        raise HTTPException(
            404, f"Document with id `{id}` does not exist in `published` collection"
        )

    await featured.document(id).set({})


@app.put("/unfeature")
async def unfeature(id: str, key: str = Depends(key_header)) -> None:
    if key != KEY:
        raise HTTPException(401)

    await featured.document(id).delete()


@app.get("/features")
async def list_featured() -> list[Paper]:
    # iterates through all the paper id in featured collection, requests for the paper data, and returns it

    return [
        Paper.model_validate((await published.document(paper_code.id).get()).to_dict())
        async for paper_code in featured.stream()
    ]


@app.post("/journal")
async def publish_journal(
    title: str, doc: UploadFile, key: str = Depends(key_header)
) -> None:
    if key != KEY:
        raise HTTPException(401)
    if doc.content_type != "application/pdf":
        raise HTTPException(415, "Please upload `.pdf` files only")
    if await aiofiles.os.path.exists(f"{DOCS_PATH}/journals/{title}"):
        raise HTTPException(422, "A publication with that title already exists")

    async with aiofiles.open(f"{DOCS_PATH}/journals/{title}", "wb") as file:
        await file.write(await doc.read(-1))
    await doc.close()
