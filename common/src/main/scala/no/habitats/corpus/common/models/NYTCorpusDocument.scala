package no.habitats.corpus.common.models

import java.io.{File, Serializable}
import java.net.URL
import java.util
import java.util.Date

/**
  * Created by mail on 03.05.2016.
  */
class NYTCorpusDocument extends Serializable {
  protected var alternateURL            : URL               = null
  protected var articleAbstract         : String            = null
  protected var authorBiography         : String            = null
  protected var banner                  : String            = null
  protected var biographicalCategories  : util.List[String] = new util.ArrayList[String]
  protected var body                    : String            = null
  protected var byline                  : String            = null
  protected var columnName              : String            = null
  protected var columnNumber            : Integer           = null
  protected var correctionDate          : Date              = null
  protected var correctionText          : String            = null
  protected var credit                  : String            = null
  protected var dateline                : String            = null
  protected var dayOfWeek               : String            = null
  protected var descriptors             : util.List[String] = new util.ArrayList[String]
  protected var featurePage             : String            = null
  protected var generalOnlineDescriptors: util.List[String] = new util.ArrayList[String]
  protected var guid                    : Int               = 0
  protected var headline                : String            = null
  protected var kicker                  : String            = null
  protected var leadParagraph           : String            = null
  protected var locations               : util.List[String] = new util.ArrayList[String]
  protected var names                   : util.List[String] = new util.ArrayList[String]
  protected var newsDesk                : String            = null
  protected var normalizedByline        : String            = null
  protected var onlineDescriptors       : util.List[String] = new util.ArrayList[String]
  protected var onlineHeadline          : String            = null
  protected var onlineLeadParagraph     : String            = null
  protected var onlineLocations         : util.List[String] = new util.ArrayList[String]
  protected var onlineOrganizations     : util.List[String] = new util.ArrayList[String]
  protected var onlinePeople            : util.List[String] = new util.ArrayList[String]
  protected var onlineSection           : String            = null
  protected var onlineTitles            : util.List[String] = new util.ArrayList[String]
  protected var organizations           : util.List[String] = new util.ArrayList[String]
  protected var page                    : Integer           = null
  protected var people                  : util.List[String] = new util.ArrayList[String]
  protected var publicationDate         : Date              = null
  protected var publicationDayOfMonth   : Integer           = null
  protected var publicationMonth        : Integer           = null
  protected var publicationYear         : Integer           = null
  protected var section                 : String            = null
  protected var seriesName              : String            = null
  protected var slug                    : String            = null
  protected var sourceFile              : File              = null
  protected var taxonomicClassifiers    : util.List[String] = new util.ArrayList[String]
  protected var titles                  : util.List[String] = new util.ArrayList[String]
  protected var typesOfMaterial         : util.List[String] = new util.ArrayList[String]
  protected var url                     : URL               = null
  protected var wordCount               : Integer           = null
  def getAlternateURL: URL = {
    return alternateURL
  }
  def getArticleAbstract: String = {
    return articleAbstract
  }
  def getAuthorBiography: String = {
    return authorBiography
  }
  def getBanner: String = {
    return banner
  }
  def getBiographicalCategories: util.List[String] = {
    return biographicalCategories
  }
  def getBody: String = {
    return body
  }
  def getByline: String = {
    return byline
  }
  def getColumnName: String = {
    return columnName
  }
  def getColumnNumber: Integer = {
    return columnNumber
  }
  def getCorrectionDate: Date = {
    return correctionDate
  }
  def getCorrectionText: String = {
    return correctionText
  }
  def getCredit: String = {
    return credit
  }
  def getDateline: String = {
    return dateline
  }
  def getDayOfWeek: String = {
    return dayOfWeek
  }
  def getDescriptors: util.List[String] = {
    return descriptors
  }
  def getFeaturePage: String = {
    return featurePage
  }
  def getGeneralOnlineDescriptors: util.List[String] = {
    return generalOnlineDescriptors
  }
  def getGuid: Int = {
    return guid
  }
  def getHeadline: String = {
    return headline
  }
  def getKicker: String = {
    return kicker
  }
  def getLeadParagraph: String = {
    return leadParagraph
  }
  def getLocations: util.List[String] = {
    return locations
  }
  def getNames: util.List[String] = {
    return names
  }
  def getNewsDesk: String = {
    return newsDesk
  }
  def getNormalizedByline: String = {
    return normalizedByline
  }
  def getOnlineDescriptors: util.List[String] = {
    return onlineDescriptors
  }
  def getOnlineHeadline: String = {
    return onlineHeadline
  }
  def getOnlineLeadParagraph: String = {
    return onlineLeadParagraph
  }
  def getOnlineLocations: util.List[String] = {
    return onlineLocations
  }
  def getOnlineOrganizations: util.List[String] = {
    return onlineOrganizations
  }
  def getOnlinePeople: util.List[String] = {
    return onlinePeople
  }
  def getOnlineSection: String = {
    return onlineSection
  }
  def getOnlineTitles: util.List[String] = {
    return onlineTitles
  }
  def getOrganizations: util.List[String] = {
    return organizations
  }
  def getPage: Integer = {
    return page
  }
  def getPeople: util.List[String] = {
    return people
  }
  def getPublicationDate: Date = {
    return publicationDate
  }
  def getPublicationDayOfMonth: Integer = {
    return publicationDayOfMonth
  }
  def getPublicationMonth: Integer = {
    return publicationMonth
  }
  def getPublicationYear: Integer = {
    return publicationYear
  }
  def getSection: String = {
    return section
  }
  def getSeriesName: String = {
    return seriesName
  }
  def getSlug: String = {
    return slug
  }
  def getSourceFile: File = {
    return sourceFile
  }
  def getTaxonomicClassifiers: util.List[String] = {
    return taxonomicClassifiers
  }
  def getTitles: util.List[String] = {
    return titles
  }
  def getTypesOfMaterial: util.List[String] = {
    return typesOfMaterial
  }
  def getUrl: URL = {
    return url
  }
  def getWordCount: Integer = {
    return wordCount
  }
  private def ljust(s: String, length1: Integer): String = {
    var length = length1
    if (s.length >= length) {
      return s
    }
    length -= s.length
    val sb: StringBuffer = new StringBuffer
    var i: Integer = 0
    while (i < length) {
      {
        sb.append(" ")
      }
      {i += 1; i - 1}
    }
    return s + sb.toString
  }
  def setAlternateURL(alternateURL: URL) {
    this.alternateURL = alternateURL
  }
  def setArticleAbstract(articleAbstract: String) {
    this.articleAbstract = articleAbstract
  }
  def setAuthorBiography(authorBiography: String) {
    this.authorBiography = authorBiography
  }
  def setBanner(banner: String) {
    this.banner = banner
  }
  def setBiographicalCategories(biographicalCategories: util.List[String]) {
    this.biographicalCategories = biographicalCategories
  }
  def setBody(body: String) {
    this.body = body
  }
  def setByline(byline: String) {
    this.byline = byline
  }
  def setColumnName(columnName: String) {
    this.columnName = columnName
  }
  def setColumnNumber(columnNumber: Integer) {
    this.columnNumber = columnNumber
  }
  def setCorrectionDate(correctionDate: Date) {
    this.correctionDate = correctionDate
  }
  def setCorrectionText(correctionText: String) {
    this.correctionText = correctionText
  }
  def setCredit(credit: String) {
    this.credit = credit
  }
  def setDateline(dateline: String) {
    this.dateline = dateline
  }
  def setDayOfWeek(dayOfWeek: String) {
    this.dayOfWeek = dayOfWeek
  }
  def setDescriptors(descriptors: util.List[String]) {
    this.descriptors = descriptors
  }
  def setFeaturePage(featurePage: String) {
    this.featurePage = featurePage
  }
  def setGeneralOnlineDescriptors(generalOnlineDescriptors: util.List[String]) {
    this.generalOnlineDescriptors = generalOnlineDescriptors
  }
  def setGuid(guid: Int) {
    this.guid = guid
  }
  def setHeadline(headline: String) {
    this.headline = headline
  }
  def setKicker(kicker: String) {
    this.kicker = kicker
  }
  def setLeadParagraph(leadParagraph: String) {
    this.leadParagraph = leadParagraph
  }
  def setLocations(locations: util.List[String]) {
    this.locations = locations
  }
  def setNames(names: util.List[String]) {
    this.names = names
  }
  def setNewsDesk(newsDesk: String) {
    this.newsDesk = newsDesk
  }
  def setNormalizedByline(normalizedByline: String) {
    this.normalizedByline = normalizedByline
  }
  def setOnlineDescriptors(onlineDescriptors: util.List[String]) {
    this.onlineDescriptors = onlineDescriptors
  }
  def setOnlineHeadline(onlineHeadline: String) {
    this.onlineHeadline = onlineHeadline
  }
  def setOnlineLeadParagraph(onlineLeadParagraph: String) {
    this.onlineLeadParagraph = onlineLeadParagraph
  }
  def setOnlineLocations(onlineLocations: util.List[String]) {
    this.onlineLocations = onlineLocations
  }
  def setOnlineOrganizations(onlineOrganizations: util.List[String]) {
    this.onlineOrganizations = onlineOrganizations
  }
  def setOnlinePeople(onlinePeople: util.List[String]) {
    this.onlinePeople = onlinePeople
  }
  def setOnlineSection(onlineSection: String) {
    this.onlineSection = onlineSection
  }
  def setOnlineTitles(onlineTitles: util.List[String]) {
    this.onlineTitles = onlineTitles
  }
  def setOrganizations(organizations: util.List[String]) {
    this.organizations = organizations
  }
  def setPage(page: Integer) {
    this.page = page
  }
  def setPeople(people: util.List[String]) {
    this.people = people
  }
  def setPublicationDate(publicationDate: Date) {
    this.publicationDate = publicationDate
  }
  def setPublicationDayOfMonth(publicationDayOfMonth: Integer) {
    this.publicationDayOfMonth = publicationDayOfMonth
  }
  def setPublicationMonth(publicationMonth: Integer) {
    this.publicationMonth = publicationMonth
  }
  def setPublicationYear(publicationYear: Integer) {
    this.publicationYear = publicationYear
  }
  def setSection(section: String) {
    this.section = section
  }
  def setSeriesName(seriesName: String) {
    this.seriesName = seriesName
  }
  def setSlug(slug: String) {
    this.slug = slug
  }
  def setSourceFile(sourceFile: File) {
    this.sourceFile = sourceFile
  }
  def setTaxonomicClassifiers(taxonomicClassifiers: util.List[String]) {
    this.taxonomicClassifiers = taxonomicClassifiers
  }
  def setTitles(titles: util.List[String]) {
    this.titles = titles
  }
  def setTypesOfMaterial(typesOfMaterial: util.List[String]) {
    this.typesOfMaterial = typesOfMaterial
  }
  def setUrl(url: URL) {
    this.url = url
  }
  def setWordCount(wordCount: Integer) {
    this.wordCount = wordCount
  }
  override def toString: String = {
    val sb: StringBuffer = new StringBuffer
    appendProperty(sb, "alternativeURL", alternateURL)
    appendProperty(sb, "articleAbstract", articleAbstract)
    appendProperty(sb, "authorBiography", authorBiography)
    appendProperty(sb, "banner", banner)
    appendProperty(sb, "biographicalCategories", biographicalCategories)
    appendProperty(sb, "body", body)
    appendProperty(sb, "byline", byline)
    appendProperty(sb, "columnName", columnName)
    appendProperty(sb, "columnNumber", columnNumber)
    appendProperty(sb, "correctionDate", correctionDate)
    appendProperty(sb, "correctionText", correctionText)
    appendProperty(sb, "credit", credit)
    appendProperty(sb, "dateline", dateline)
    appendProperty(sb, "dayOfWeek", dayOfWeek)
    appendProperty(sb, "descriptors", descriptors)
    appendProperty(sb, "featurePage", featurePage)
    appendProperty(sb, "generalOnlineDescriptors", generalOnlineDescriptors)
    appendProperty(sb, "guid", guid)
    appendProperty(sb, "headline", headline)
    appendProperty(sb, "kicker", kicker)
    appendProperty(sb, "leadParagraph", leadParagraph)
    appendProperty(sb, "locations", locations)
    appendProperty(sb, "names", names)
    appendProperty(sb, "newsDesk", newsDesk)
    appendProperty(sb, "normalizedByline", normalizedByline)
    appendProperty(sb, "onlineDescriptors", onlineDescriptors)
    appendProperty(sb, "onlineHeadline", onlineHeadline)
    appendProperty(sb, "onlineLeadParagraph", onlineLeadParagraph)
    appendProperty(sb, "onlineLocations", onlineLocations)
    appendProperty(sb, "onlineOrganizations", onlineOrganizations)
    appendProperty(sb, "onlinePeople", onlinePeople)
    appendProperty(sb, "onlineSection", onlineSection)
    appendProperty(sb, "onlineTitles", onlineTitles)
    appendProperty(sb, "organizations", organizations)
    appendProperty(sb, "page", page)
    appendProperty(sb, "people", people)
    appendProperty(sb, "publicationDate", publicationDate)
    appendProperty(sb, "publicationDayOfMonth", publicationDayOfMonth)
    appendProperty(sb, "publicationMonth", publicationMonth)
    appendProperty(sb, "publicationYear", publicationYear)
    appendProperty(sb, "section", section)
    appendProperty(sb, "seriesName", seriesName)
    appendProperty(sb, "slug", slug)
    appendProperty(sb, "sourceFile", sourceFile)
    appendProperty(sb, "taxonomicClassifiers", taxonomicClassifiers)
    appendProperty(sb, "titles", titles)
    appendProperty(sb, "typesOfMaterial", typesOfMaterial)
    appendProperty(sb, "url", url)
    appendProperty(sb, "wordCount", wordCount)
    return sb.toString
  }
  private def appendProperty(sb: StringBuffer, propertyName: String, propertyValue1: Any) {
    var propertyValue = propertyValue1
    if (propertyValue != null) {
      propertyValue = propertyValue.toString.replaceAll("\\s+", " ").trim
    }
    sb.append(ljust(propertyName + ":", 45) + propertyValue + "\n")
  }
}
