package llc.ethereal.loom.api

class ValidationException(field: String, value: Any?, reason: String)
    : RuntimeException("validation failed for '$field' = $value: $reason")

/** A predicate over a single field's value. Return true if value is acceptable. */
typealias Validator<T> = (T) -> Boolean
