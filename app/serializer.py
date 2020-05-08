from marshmallow import Schema, fields


class ContactRequestSchema(Schema):
    name = fields.Str()
    email = fields.Str()
    message = fields.Str()


class ListservSubscribeSchema(Schema):
    name = fields.Str()
    email = fields.Str()


class TagSchema(Schema):
    tag = fields.Str()


class TermSchema(Schema):
    term = fields.Str()


class ModelCountSchema(Schema):
    model_name = fields.Str(attribute="model_name")
    count = fields.Int()


class DatasetSchema(Schema):
    id = fields.Int()
    name = fields.String()
    description = fields.String()
    ownerName = fields.String(attribute="owner_name")
    organizationName = fields.String(attribute="organization_name")
    license = fields.String()
    tags = fields.List(fields.Nested(TagSchema))
    version = fields.Int()
    size = fields.Int()
    contributors = fields.List(fields.Str())
    modelCount = fields.List(fields.Nested(ModelCountSchema), attribute="model_count")
    fileCount = fields.Int(attribute="file_count")
    recordCount = fields.Int(attribute="record_count")
    uri = fields.Str()
    arn = fields.Str()
    status = fields.Str()
    doi = fields.Str()
    banner = fields.Str()
    readme = fields.Str()
    createdAt = fields.Str(attribute="created_at")
    updatedAt = fields.Str(attribute="updated_at")


class FileSchema(Schema):
    name = fields.Str()
    datasetId = fields.Int(attribute="dataset_id")
    datasetVersion = fields.Int(attribute="dataset_version")
    size = fields.Int()
    uri = fields.Str()
    createdAt = fields.Str(attribute="created_at")


class PaginatedFileResponseSchema(Schema):
    limit = fields.Int()
    offset = fields.Int()
    files = fields.List(fields.Nested(FileSchema))
    totalCount = fields.Int(attribute="total_count")


class PaginatedDatasetResponseSchema(Schema):
    limit = fields.Int()
    offset = fields.Int()
    datasets = fields.List(fields.Nested(DatasetSchema))
    totalCount = fields.Int(attribute="total_count")
