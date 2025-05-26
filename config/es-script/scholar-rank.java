      // Define required fields
      if (doc['n_cited'] == null || doc['coarse_rank_score'] == null || doc['search_labels'] == null || 
          doc['labels_1'] == null || doc['keywords_001'] == null || doc['iso3166'] == null || 
          doc['title_score'] == null) {
        return 0;
      }

      // Get raw query from parameters
      String rawQuery = params.raw_query;

      // Get document fields
      List<String> searchLabels = doc['search_labels'];
      List<String> labels1 = doc['labels_1'];
      List<String> keywords001 = doc['keywords_001'];
      long n_cited = doc['n_cited'].value;
      double coarse_rank_score = doc['coarse_rank_score'].value;
      String iso3166 = doc['iso3166'].value;
      long title_score = doc['title_score'].value;

      // First phase score (placeholder - needs actual implementation)
      double f1 = 0; // _f1.evaluate(params) equivalent needed

      // Text relevance scores (simplified to use ElasticSearch's built-in scoring)
      double textRelevanceName = _score; // Placeholder for name field relevance
      double textRelevancePaperTitle = _score; // Placeholder for paper_title field relevance
      double textRelevancePatentTitle = _score; // Placeholder for patent_title field relevance
      double textRelevanceMajor = _score; // Placeholder for major field relevance
      double textRelevanceContent001 = _score; // Placeholder for content_001 field relevance

      // Query match ratios (simplified)
      double queryMatchRatioName = 0; // Placeholder for name field match ratio
      double queryMatchRatioPaperTitle = 0; // Placeholder for paper_title field match ratio
      double queryMatchRatioPatentTitle = 0; // Placeholder for patent_title field match ratio
      double queryMatchRatioMajor = 0; // Placeholder for major field match ratio
      double queryMatchRatioContent001 = 0; // Placeholder for content_001 field match ratio

      // Field match ratio
      double fieldMatchRatioName = 0; // Placeholder for name field match ratio

      // Query min slide window (simplified)
      double queryMinSlideWindowName = 0;
      double queryMinSlideWindowPaperTitle = 0;
      double queryMinSlideWindowPatentTitle = 0;
      double queryMinSlideWindowMajor = 0;
      double queryMinSlideWindowContent001 = 0;
      double queryMinSlideWindowAwardProject = 0;
      double queryMinSlideWindowPaperTitleSynonym = 0;

      // Field term match count
      double fieldTermMatchCountPaperTitle = 0;
      double fieldTermMatchCountPatentTitle = 0;

      // BM25F score (using ElasticSearch's built-in BM25)
      double bm25fSynonymPack = _score; // Placeholder for combined BM25 score

      // Proxima score (vector similarity)
      double proximaScoreVector = 0; // Placeholder for vector similarity score

      // Calculate text relevance score
      double textRelevanceScore = textRelevanceName * 0.4
                               + queryMatchRatioName * 0.4
                               + queryMinSlideWindowName * 0.4
                               + textRelevancePaperTitle * 0.3
                               + queryMatchRatioPaperTitle * 0.3
                               + queryMinSlideWindowPaperTitle * 0.3
                               + textRelevancePatentTitle * 0.1
                               + queryMatchRatioPatentTitle * 0.1
                               + queryMinSlideWindowPatentTitle * 0.1
                               + textRelevanceMajor * 0.4
                               + queryMatchRatioMajor * 0.4
                               + queryMinSlideWindowMajor * 0.4
                               + textRelevanceContent001 * 0.5
                               + queryMatchRatioContent001 * 0.5
                               + queryMinSlideWindowContent001 * 0.5;

      // Normalize score (simplified normalization)
      double relevanceScore = textRelevanceScore; // Actual normalization needed

      // Vector boost
      double vectorBoost = 0;
      if (proximaScoreVector < 1) {
        vectorBoost = 1 - proximaScoreVector;
        vectorBoost = vectorBoost * 0.05; // Simplified normalization
      }
      relevanceScore += vectorBoost;

      // Name or major boost
      double name_or_major_boost = 0;
      if ((queryMinSlideWindowName > 0.99 && fieldMatchRatioName > 0.99) || queryMinSlideWindowMajor > 0.99) {
        name_or_major_boost = 1;
        relevanceScore += name_or_major_boost;
      }

      // RAG boost
      double rag_boost = 0;
      if (queryMinSlideWindowContent001 > 0.99) {
        rag_boost = 2;
        relevanceScore += rag_boost;
      }

      // Awards boost
      double awards_boost = 0;
      if (queryMinSlideWindowAwardProject > 0.99) {
        awards_boost = 3;
        relevanceScore += awards_boost;
      }

      // Search labels boost
      double searchLabelsBoost = 0;
      if (searchLabels != null && rawQuery != null) {
        for (String label : searchLabels) {
          if (label.equalsIgnoreCase(rawQuery)) {
            if (searchLabelsBoost == 0) {
              searchLabelsBoost += 1;
            } else {
              searchLabelsBoost += 0.01;
            }
          }
        }
        relevanceScore += searchLabelsBoost;
      }

      // Labels1 boost
      double labels1Boost = 0;
      if (labels1 != null && rawQuery != null) {
        for (String label : labels1) {
          if (label.equalsIgnoreCase(rawQuery)) {
            if (labels1Boost == 0) {
              labels1Boost += 1;
            } else {
              labels1Boost += 0.01;
            }
          }
        }
        labels1Boost *= 0.8;
        relevanceScore += labels1Boost;
      }

      // Keywords boost
      double keywords001Boost = 0;
      if (keywords001 != null && rawQuery != null) {
        for (String keyword : keywords001) {
          if (keyword.equalsIgnoreCase(rawQuery)) {
            keywords001Boost = 20;
            break;
          }
        }
        relevanceScore += keywords001Boost;
      }

      // Cited score (simplified normalization)
      double citedScore = n_cited / 100000.0;
      if (citedScore > 1) citedScore = 1;

      // Field match scores
      double fieldMatchScorePaperTitle = fieldTermMatchCountPaperTitle / 1000.0;
      double fieldMatchScorePatentTitle = fieldTermMatchCountPatentTitle / 1000.0;

      // Activity score calculation
      double activityScore = (fieldMatchScorePaperTitle + fieldMatchScorePatentTitle) * 1.0;
      if (relevanceScore >= 1) {
        activityScore += coarse_rank_score * 0.2 + citedScore * 0.1;
        if (title_score > 0) {
          activityScore += title_score / 1000.0;
        }
      } else if (relevanceScore >= 0.5) {
        activityScore += coarse_rank_score * 0.1;
        if (title_score > 0) {
          activityScore += title_score / 10000.0;
        }
      } else {
        if (title_score > 0) {
          activityScore += title_score / 100000.0;
        }
      }
      
      relevanceScore += bm25fSynonymPack * 0.1;

      // ISO3166 boost
      double activity_score_iso3166_boost = 0;
      if (iso3166.isEmpty()) {
        activity_score_iso3166_boost = -10;
      } else if (iso3166.equalsIgnoreCase("US") || iso3166.equalsIgnoreCase("GB") || 
                 iso3166.equalsIgnoreCase("CA") || iso3166.equalsIgnoreCase("AU") || 
                 iso3166.equalsIgnoreCase("NZ")) {
        if (relevanceScore >= 0.5) {
          activity_score_iso3166_boost = coarse_rank_score * 0.1;
        } else {
          activity_score_iso3166_boost = coarse_rank_score * 0.01;
        }
      }
      activityScore += activity_score_iso3166_boost;

      // Total score
      double score = relevanceScore + activityScore;

      // Return the final score
      return score;
      