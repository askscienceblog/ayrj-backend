import re
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, Form, HTTPException, UploadFile
from fastapi.security import APIKeyHeader
from google.cloud import firestore
from pydantic import BaseModel

# used to authenticate access to restricted parts of this api
key_header = APIKeyHeader(name="x-ayrj-key", auto_error=False)
KEY = "In science we find truth, @ ayrj we have this API"  # protect at all cost. if this leaks, we have lost everything

# regex to match doi urls so that they can be automatically extracted
# this is not foul-proof, there is no way to check if the matched url is valid without actually requesting doi.org
MATCH_DOI = re.compile("10\\.[\\.0-9]+/\\S*\\w")

db = firestore.AsyncClient(project="key-being-411010")

# use the same collection id so that all papers can be searched with a collection group query
papers = db.collection("papers")
reviewing = papers.document("reviewing").collection("paper-data")
published = papers.document("published").collection("paper-data")
retracted = papers.document("retracted").collection("paper-data")

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
            raise HTTPException(400, "No authors provided")
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
        id += 1
        id %= 1_000_000_000
        code = format_id_to_string(id)

    return code


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


app = FastAPI()


@app.get("/")
async def welcome() -> str:
    return "Welcome to the AYRJ backend API"


@app.post("/submit")
async def submit(
    title: Annotated[str, Form()],
    abstract: Annotated[str, Form()],
    authors: Annotated[list[str], Form()],
    category: Annotated[str, Form()],
    references: Annotated[list[str], Form()],
    doc: UploadFile,
    key: str = Depends(key_header),
) -> None:
    """Handles initialising all paper data and adding it to the `reviewing` collection"""

    if key != KEY:
        raise HTTPException(401)

    if (
        doc.content_type != "application/msword"
        and doc.content_type
        != "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ):
        raise HTTPException(400, "Upload `.doc` or `.docx` files only")

    # shorthand for authors, also validates that `authors` is non-empty
    shorthand = generate_author_shorthand(authors)

    doc_bytes = await doc.read(-1)
    await doc.close()

    code = await generate_unique_document_id(doc_bytes)

    # upload document
    await document_repo.document(code).set(
        {
            "filename": f"{shorthand} DRAFT.docx",
            "bytes": doc_bytes,
        }
    )
    del doc_bytes  # free memory since file may be large

    # create the `Paper` object, using pydantic parser to enforce type checking
    paper = Paper(
        id=code,
        title=title,
        abstract=abstract,
        authors=authors,
        category=category,
        references=references,
        submitted=datetime.now(tz=timezone.utc),
    )

    await reviewing.document(code).set(paper.model_dump())


@app.put("/publish")
async def publish(id: str, key: str = Depends(key_header)) -> None:
    """Moves paper from `reviewing` to `published` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = reviewing.document(id)
    paper = await paper_ref.get()

    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in review list")

    now = datetime.now(tz=timezone.utc)
    paper = Paper.model_validate(paper.to_dict())
    paper.published = now

    await published.document(id).set(paper.model_dump())
    await paper_ref.delete()

    await document_repo.document(id).update(
        {
            "filename": f"{generate_author_shorthand(paper.authors)} ({now.strftime("%Y")}).pdf"
        }
    )


@app.delete("/reject")
async def reject(id: str, key: str = Depends(key_header)) -> None:
    """Removes paper from `paper-data` collection and documents from `documents` colelction"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = reviewing.document(id)
    paper = await paper_ref.get()

    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in review list")
    await paper_ref.delete()
    await document_repo.document(id).delete()


@app.patch("/review")
async def review(
    id: Annotated[str, Form()],
    title: Annotated[str | None, Form()] = None,
    abstract: Annotated[str | None, Form()] = None,
    authors: Annotated[list[str] | None, Form()] = None,
    category: Annotated[str | None, Form()] = None,
    references: Annotated[list[str] | None, Form()] = None,
    doc: UploadFile | None = None,
    key: str = Depends(key_header),
) -> None:
    """Updates paper in `reviewing` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = reviewing.document(id)
    paper = await paper_ref.get()

    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in reviewing list")

    if doc is not None:
        if (
            doc.content_type == "application/msword"
            or doc.content_type
            == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ):
            extension = "docx"
        elif doc.content_type == "application/pdf":
            extension = "pdf"
        else:
            raise HTTPException(400, "Upload `.pdf`, `.doc` or `.docx` files only")

        await document_repo.document(id).update(
            {
                "filename": f"{generate_author_shorthand(paper.authors)} DRAFT.{extension}",
                "bytes": await doc.read(-1),
            }
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

    if update_dict or doc is not None:
        await paper_ref.update(
            update_dict
            | {"reviewed": firestore.ArrayUnion([datetime.now(tz=timezone.utc)])}
        )


@app.put("/retract")
async def retract(id: str, key: str = Depends(key_header)) -> None:
    """Moves paper from `reviewing` collection to `retracted` collection
    Retracts paper but does not actually delete any data"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = published.document(id)
    paper = await paper_ref.get()

    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in published list")

    paper = Paper.model_validate(paper.to_dict())
    paper.retracted = datetime.now(tz=timezone.utc)

    await retracted.document(id).set(paper.model_dump())
    await paper_ref.delete()


@app.delete("/remove")
async def remove(id: str, key: str = Depends(key_header)) -> None:
    """Removes retracted paper data from `retracted` and `documents` collection"""

    if key != KEY:
        raise HTTPException(401)

    paper_ref = retracted.document(id)
    paper = await paper_ref.get()
    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in retracted list")

    await paper_ref.delete()
    # remove all document data but leave the document name, this will prevent retracted `id`s from being reused
    await document_repo.document(id).set({})


@app.post("/correct")
async def correct(
    id: Annotated[str, Form()],
    description: Annotated[str, Form()],
    doc: UploadFile,
    key: str = Depends(key_header),
) -> None:
    "Adds a correction to a published paper"

    if key != KEY:
        raise HTTPException(401)

    if doc.content_type != "application/pdf":
        raise HTTPException(400, "Please upload only `.pdf` files")

    paper_ref = published.document(id)
    paper = await paper_ref.get()
    if not paper.exists:
        raise HTTPException(400, "Paper does not exist in published list")

    doc_bytes = await doc.read(-1)
    await doc.close()
    code = generate_unique_document_id(doc_bytes)

    await document_repo.document(code).set(
        {
            "filename": f"{generate_author_shorthand(paper.authors)} Correction {len(paper.corrected)+1}.pdf",
            "bytes": doc_bytes,
        }
    )

    await paper_ref.update(
        {
            "corrected": firestore.ArrayUnion(
                [
                    Correction(
                        id=id,
                        date=datetime.now(tz=timezone.utc),
                        description=description,
                    )
                ]
            )
        }
    )
