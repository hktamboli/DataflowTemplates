package com.google.cloud.teleport.v2.neo4j.model.helpers;

import org.apache.commons.csv.CSVFormat;
import org.neo4j.importer.v1.sources.TextFormat;

public class CsvSources {
    public static CSVFormat toCsvFormat(TextFormat format) {
      switch (format) {
        case EXCEL:
          return CSVFormat.EXCEL;
        case INFORMIX:
          return CSVFormat.INFORMIX_UNLOAD_CSV;
        case MONGO:
          return CSVFormat.MONGODB_CSV;
        case MONGO_TSV:
          return CSVFormat.MONGODB_TSV;
        case MYSQL:
          return CSVFormat.MYSQL;
        case ORACLE:
          return CSVFormat.ORACLE;
        case POSTGRES:
          return CSVFormat.POSTGRESQL_TEXT;
        case POSTGRESQL_CSV:
          return CSVFormat.POSTGRESQL_CSV;
        case RFC4180:
          return CSVFormat.RFC4180;
        case DEFAULT:
        default:
          return CSVFormat.DEFAULT;
      }
    }
}
