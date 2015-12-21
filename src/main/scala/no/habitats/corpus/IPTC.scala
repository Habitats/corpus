package no.habitats.corpus

import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.vocabulary.SKOS

import scala.collection.JavaConverters._
import scala.io.Source

object IPTC {
  // Helper methods for IPTC/RDF
  lazy val model: Model = {
    val model = ModelFactory.createDefaultModel()
    val name = getClass.getResourceAsStream("/cptall-en-GB.rdf")
    model.read(name, null)
  }

  //  def toToplevelResource(x: Resource): Resource = if (x.hasProperty(SKOS.broader) && x.getProperty(SKOS.broader).getResource != x) toToplevelResource(x.getProperty(SKOS.broader).getResource) else x

  def toToplevelResource(level: Int, child: Resource, oldChildren: Seq[Resource] = Seq()): Resource = {
    if (child.hasProperty(SKOS.broader) && child.getProperty(SKOS.broader).getResource != child) {
      val parent = child.getProperty(SKOS.broader).getResource
      toToplevelResource(level, parent, Seq(child) ++ oldChildren)
    } else {
      if (level == 0 || oldChildren.isEmpty) child
      else if (oldChildren.size < level) oldChildren.last
      else oldChildren(level - 1)
    }
  }

  def name(r: Resource) = r.getProperty(SKOS.prefLabel).getString.trim

  def id(r: Resource) = r.getURI.split("/").last

  // ##################
  // ### IPTC STUFF ###
  // ##################

  // [ID, MediaTopic] pairs, contaning only top level media topics
  def topLevelMediaTopics(level: Int): Map[String, String] = {
    model.listSubjects.asScala.filter(_.hasProperty(SKOS.prefLabel)).map(c => {
      val top = toToplevelResource(level, c)
      (name(c), name(top))
    }).toMap
  }

  // [ID, MediaTopic] pairs
  lazy val allMediaTopics: Map[String, String] = model.listSubjects.asScala.filter(_.hasProperty(SKOS.prefLabel)).map(s => (id(s), name(s))).toMap
  // [MediaTopic]
  lazy val mediaTopicLabels: Set[String] = allMediaTopics.values.toSet
  // [NewYorkTimesDescriptor, MediaTopic] pairs
  lazy val nytToIptc: Map[String, String] = {
    val f = Source.fromInputStream(getClass.getResourceAsStream("/nyt_to_iptc.txt")).getLines()
    f.map(_.split("\t").toList)
      .filter(_ (3) != "n/a")
      // extract [NewYorkTimeDescriptor, MediaTopicID]
      .map(concept => (concept.head.toLowerCase, concept(3).split("/").last)).toMap
  }

  // Map every descriptor to its corresponding IPTC topic
  def toIptc(desc: Set[String]): Set[String] = desc.map(_.toLowerCase).map(d => {
    // Is it defined in the nyt mapping?
    if (nytToIptc.contains(d)) {
      val id = nytToIptc.get(d).get
      allMediaTopics.contains(id) match {
        case true =>
          val topic = allMediaTopics(id)
          //          Log.v(d + " -> " + topLevelMediaTopics(topic) )
          //          Log.v(d + " -> " + topic )
          if (topic == "travel") None
          else Some(topic)
        case false => Log.v(s"ID: $id missing in IPTC ..."); None
      }
    }
    // Is it defined in the official MediaTopics?
    else if (mediaTopicLabels.contains(d)) {
      Some(d)
    }
    else None
  }).filter(_.nonEmpty).map(_.get)

  // Map every descriptor to its corresponding top level IPTC topic
  lazy val levels: Map[Int, Map[String, String]] = {
    (for {
      level <- 0 to 5
    } yield (level -> topLevelMediaTopics(level))).toMap
  }

  def toBroad(desc: Set[String], level: Int): Set[String] = toIptc(desc).map(levels(level))
}

