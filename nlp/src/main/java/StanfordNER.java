import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import no.habitats.corpus.common.Log;

public class StanfordNER {

  private StanfordCoreNLP pipeline;

  public StanfordNER() {
    Log.v("Initializing Annotator pipeline ...");

    Properties props = new Properties();

    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");

    pipeline = new StanfordCoreNLP(props);

    Log.v("Annotator pipeline initialized");
  }

  List<String> findNameEntityType(String text, String entity) {
    Log.v("Finding entity type matches in the " + text + " for entity type, " + entity);

    // create an empty Annotation just with the given text
    Annotation document = new Annotation(text);

    // run all Annotators on this text
    pipeline.annotate(document);
    List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
    List<String>  matches   = new ArrayList<String>();

    for (CoreMap sentence : sentences) {

      int previousCount = 0;
      int count         = 0;
      // traversing the words in the current sentence
      // a CoreLabel is a CoreMap with additional token-specific methods

      for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
        String word = token.get(CoreAnnotations.TextAnnotation.class);

        int previousWordIndex;
        if (entity.equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
          count++;
          if (previousCount != 0 && (previousCount + 1) == count) {
            previousWordIndex = matches.size() - 1;
            String previousWord = matches.get(previousWordIndex);
            matches.remove(previousWordIndex);
            previousWord = previousWord.concat(" " + word);
            matches.add(previousWordIndex, previousWord);

          } else {
            matches.add(word);
          }
          previousCount = count;
        } else {
          count = 0;
          previousCount = 0;
        }


      }

    }
    return matches;
  }
}
