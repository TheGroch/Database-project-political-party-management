WITH whole_upvotes AS
(
    SELECT actions.id, COUNT(actions.id) as upvotes
    FROM actions JOIN votes ON actions.id = votes.action_ID
    WHERE vote_type = 'upvote'
    GROUP BY actions.id
), whole_downvotes AS
(
    SELECT actions.id, COUNT(actions.id) as downvotes
    FROM actions JOIN votes ON actions.id = votes.action_ID
    WHERE vote_type = 'downvote'
    GROUP BY actions.id
)
SELECT actions.id,
action_type,
projects.id as project_ID,
authority_ID,
CASE WHEN upvotes IS NULL THEN 0 else upvotes END AS upvotes,
CASE WHEN downvotes IS NULL THEN 0 else downvotes END AS downvotes  
FROM whole_upvotes FULL JOIN whole_downvotes ON whole_upvotes.id = whole_downvotes.id
JOIN actions ON whole_downvotes.id = actions.id
JOIN projects ON actions.project_ID = projects.id
