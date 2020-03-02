 WITH whole_upvotes AS
(
    SELECT member.id, COUNT(member.id) as upvotes
    FROM member JOIN votes ON member.id = votes.member_ID
    WHERE vote_type = 'upvote' AND action_ID = %s
    GROUP BY member.id
), whole_downvotes AS
(
    SELECT member.id, COUNT(member.id) as downvotes
    FROM member JOIN votes ON member.id = votes.member_ID
    WHERE vote_type = 'downvote' AND action_ID = %s
    GROUP BY member.id
)
SELECT member.id, 
CASE WHEN whole_upvotes.upvotes IS NULL THEN 0 ELSE whole_upvotes.upvotes END AS upvotes,
CASE WHEN whole_downvotes.downvotes IS NULL THEN 0 ELSE whole_downvotes.downvotes END AS downvotes FROM
member LEFT JOIN whole_upvotes ON member.id = whole_upvotes.id
LEFT JOIN whole_downvotes ON whole_downvotes.id = member.id
ORDER BY member.id ASC;