package io

package object dstlr {

  // Result from spark-solr
  case class SolrRow(id: String, contents: String)

  // Result of our extraction
  case class TripleRow(doc: String, subjectType: String, subjectValue: String, relation: String, objectType: String, objectValue: String)

  // Mapping from WikiData properties to our relation names
  case class WikiDataMappingRow(property: String, relation: String)

}