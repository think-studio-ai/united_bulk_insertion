
from pydantic import BaseModel, Field, field_validator


class BulkModel(BaseModel):
    path: str = Field(..., description="The path of the file to bulk insert data from")
    type: str = Field(..., description="The type of data to bulk insert")


    @field_validator('type')
    def check_type(cls, value):
        if value not in ["beneficial", "volunteer", "coordinators"]:
            raise ValueError("Type must be one of 'beneficial', 'volunteer', or 'coordinators'")
        return value
