/*
This is a scheduled SQL script on BigQuery that will push the Data from the Staging DBs to 
a consolidated DB with all the necessary information
*/

START TRANSACTION;

-- Insert new Posts into Posts Table

INSERT INTO reddit.Posts (Id, favourites, comment_count, Body, Title, platform)
SELECT rs.id, rs.score, rs.num_comments, rs.body, rs.title, 'Reddit'
FROM (
    SELECT id, score, num_comments, body, title,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM reddit.reddit_scraped
) AS rs
WHERE rs.rn = 1 AND NOT EXISTS (
    SELECT 1 FROM reddit.Posts p WHERE p.Id = rs.id
);

INSERT INTO reddit.Posts (Id, Body, Title, keywords, platform)
SELECT ns.checksum, ns.cleaned_text, ns.headline, ns.keywords, 'News'
FROM (
    SELECT checksum, cleaned_text, headline, keywords,
           ROW_NUMBER() OVER (PARTITION BY checksum ORDER BY checksum) AS rn
    FROM reddit.news_scraped
) AS ns
WHERE ns.rn = 1 AND NOT EXISTS (
    SELECT 1 FROM reddit.Posts p WHERE p.Id = ns.checksum
);

INSERT INTO reddit.Posts (Id, favourites, comment_count, Location, Body, platform)
SELECT ts.id, ts.favorite_count, ts.reply_count, ts.point, ts.text, 'Twitter'
FROM (
    SELECT id, favorite_count, reply_count, point, text,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn
    FROM reddit.twitter_scraped
) AS ts
WHERE ts.rn = 1 AND NOT EXISTS (
    SELECT 1 FROM reddit.Posts p WHERE p.Id = ts.id
);


-- -- Insert into CandidatePosts Table (Need to check with new Data)
INSERT INTO reddit.CandidatePost (Candidate_ID, Post_ID, overall_sentiment, sentiment_score)
SELECT c.ID, p.Id , t.sentiment,t.sentiment_score
FROM reddit.twitter_scraped t
JOIN reddit.Candidate c ON t.topic = c.Name
JOIN reddit.Posts p ON t.id = p.Id
WHERE NOT EXISTS (
    SELECT 1 FROM reddit.CandidatePost cp WHERE cp.Candidate_ID = c.ID AND cp.Post_ID = p.Id
)
UNION ALL
SELECT c.ID, p.Id , r.sentiment,r.sentiment_score
FROM reddit.reddit_scraped r
JOIN reddit.Candidate c ON r.topic = c.Name
JOIN reddit.Posts p ON r.id = p.Id
WHERE NOT EXISTS (
    SELECT 1 FROM reddit.CandidatePost cp WHERE cp.Candidate_ID = c.ID AND cp.Post_ID = p.Id
)
UNION ALL
SELECT c.ID, p.Id , n.sentiment,n.sentiment_score
FROM reddit.news_scraped n
JOIN reddit.Candidate c ON n.topic = c.Name
JOIN reddit.Posts p ON n.checksum = p.Id
WHERE NOT EXISTS (
    SELECT 1 FROM reddit.CandidatePost cp WHERE cp.Candidate_ID = c.ID AND cp.Post_ID = p.Id
);

COMMIT;
