getSerdeType <- function(object) {
  type <- class(object)[[1]]

  if (type != "list") {
    type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) {
      getSerdeType(elem)
    }))
    if (length(elemType) <= 1) {

      # Check that there are no NAs in arrays since they are unsupported in scala
      hasNAs <- any(is.na(object))

      if (hasNAs) {
        "list"
      } else {
        "array"
      }
    } else {
      "list"
    }
  }
}

writeObject <- function(con, object, writeType = TRUE) {
  type <- class(object)[[1]]

  if (type %in% c("integer", "character", "logical", "double", "numeric", "factor", "Date", "POSIXct")) {
    if (is.na(object) && !is.nan(object)) {
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(serdeType,
    NULL = writeVoid(con),
    integer = writeInt(con, object),
    character = writeString(con, object),
    logical = writeBoolean(con, object),
    double = writeDouble(con, object),
    numeric = writeDouble(con, object),
    raw = writeRaw(con, object),
    array = writeArray(con, object),
    list = writeList(con, object),
    struct = writeList(con, object),
    spark_jobj = writeJobj(con, object),
    environment = writeEnv(con, object),
    Date = writeDate(con, object),
    POSIXlt = writeTime(con, object),
    POSIXct = writeTime(con, object),
    factor = writeFactor(con, object),
    `data.frame` = writeList(con, object),
    spark_apply_binary_result = writeList(con, object),
    json = writeJson(con, object),
    stop("Unsupported type '", serdeType, "' for serialization")
  )
}

writeVoid <- function(con) {
  # no value for NULL
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(as.character(value))
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

writeType <- function(con, class) {
  type <- switch(class,
    NULL = "n",
    integer = "i",
    character = "c",
    logical = "b",
    double = "d",
    numeric = "d",
    raw = "r",
    array = "a",
    list = "l",
    struct = "s",
    spark_jobj = "j",
    environment = "e",
    Date = "D",
    POSIXlt = "t",
    POSIXct = "t",
    factor = "c",
    `data.frame` = "l",
    spark_apply_binary_result = "l",
    json = "J",
    stop("Unsupported type '", class, "' for serialization")
  )
  writeBin(charToRaw(type), con)
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) {
      env[[x]]
    })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeInt(con, as.integer(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}

writeFactor <- function(con, factor) {
  writeString(con, as.character(factor))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}

writeJson <- function(con, object) {
  writeString(con, object)  
}

assignInNamespace('writeObject', writeObject, 'sparklyr')
assignInNamespace('writeType', writeType, 'sparklyr')
