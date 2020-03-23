select
		 t.TriggererSendDefinitionObjectID AS [JobID]
		,t.[Email Name]
		,t.[Delivered Timestamp]
		,t.EmailAddress
		,t.SubscriberKey
		,t.[Delivered]
		,t.[Open Timestamp]
		,t.[Open]
		,t.[Send Timestamp]
		,t.Send
		,t.Bounced
		,t.[Bounced Timestamp]
		,t.Click
		,t.[Click Timestamp]
from
				(
					Select
							 j.TriggererSendDefinitionObjectID
							,j.EmailName as [Email Name]
							,j.DeliveredTime as [Delivered Timestamp]
							,s.EmailAddress
							,s.SubscriberKey
							,'TRUE' as [Delivered]
							,CASE
								When o.EventDate <>'' then 'TRUE' Else NULL end as [Open]
							,o.EventDate as [Open Timestamp]
							,'TRUE' as Click
							,cl.EventDate as [Click Timestamp]
							,bnc.EventDate as [Bounced Timestamp]
							,CASE
								When bnc.EventDate <> '' then 'TRUE' Else NULL end as [Bounced]
							,sn.EventDate as [Send Timestamp]
							,'TRUE' as Send
					from [_Click] cl
							inner join [_Job] j on j.TriggererSendDefinitionObjectID = cl.TriggererSendDefinitionObjectID
							inner join [_Subscribers] s on cl.SubscriberKey = s.SubscriberKey
							inner join [Experienced Nudge Group] cm on cl.SubscriberKey = cm.[Deloitte Email Address]
							left join [_Sent] sn on cl.SubscriberKey = sn.SubscriberKey and cl.TriggererSendDefinitionObjectID = sn.TriggererSendDefinitionObjectID
							left join [_Open] o on cl.SubscriberKey = o.SubscriberKey and cl.TriggererSendDefinitionObjectID = o.TriggererSendDefinitionObjectID and o.IsUnique = 1
							left join [_bounce] bnc on cl.SubscriberKey = bnc.SubscriberKey and cl.TriggererSendDefinitionObjectID = bnc.TriggererSendDefinitionObjectID and bnc.IsUnique = 1
					where
					 		cl.IsUnique=1 and
								(
					  			cl.EventDate between dateadd(d,-7,getdate()) and getdate()
								)

							UNION

					Select
					  	  j.TriggererSendDefinitionObjectID
							 ,j.EmailName as [Email Name]
							 ,j.DeliveredTime as [Delivered Timestamp]
							 ,s.EmailAddress
							 ,s.SubscriberKey
							 ,'TRUE' as [Delivered]
							 ,'TRUE' as [Open]
							 ,o.EventDate as [Open Timestamp]
							 ,CASE
										When cl.EventDate <>'' then 'TRUE' Else NULL end  as Click
							 ,cl.EventDate as [Click Timestamp]
							 ,bnc.EventDate as [Bounced Timestamp]
							 ,CASE
							 			When bnc.EventDate <> '' then 'TRUE'
										Else NULL end as [Bounced]
							 ,sn.EventDate as [Send Timestamp]
							 ,'TRUE' as Send
					from [_Open] o
							 inner join [_Job] j on o.TriggererSendDefinitionObjectID = j.TriggererSendDefinitionObjectID
							 inner join [_Subscribers] s on o.SubscriberKey = s.SubscriberKey
							 inner join [Experienced Nudge Group] cm on o.SubscriberKey = cm.[Deloitte Email Address]
							 left join [_Sent] sn on o.SubscriberKey = sn.SubscriberKey and o.TriggererSendDefinitionObjectID = sn.TriggererSendDefinitionObjectID
							 left join [_Click] cl on o.SubscriberKey = cl.SubscriberKey and o.TriggererSendDefinitionObjectID = cl.TriggererSendDefinitionObjectID and cl.IsUnique = 1
							 left join [_bounce] bnc on o.SubscriberKey = bnc.SubscriberKey and o.TriggererSendDefinitionObjectID = bnc.TriggererSendDefinitionObjectID
					where
							o.IsUnique = 1 and
								(
					  				o.EventDate between dateadd(d,-7,getdate()) and getdate()
								)

								UNION

					Select
								 j.TriggererSendDefinitionObjectID
								,j.EmailName as [Email Name]
								,j.DeliveredTime as [Delivered Timestamp]
								,s.EmailAddress
								,s.SubscriberKey
								,'TRUE' as [Delivered]
								,CASE
										When  o.EventDate <> '' then 'TRUE' Else NULL end as [Open]
								,o.EventDate as [Open Timestamp]
								,CASE
										When cl.EventDate <>'' then 'TRUE' else NULL end  as Click
								,cl.EventDate as [Click Timestamp]
								,bnc.EventDate as [Bounced Timestamp]
								,CASE
										When bnc.EventDate <> '' then 'TRUE' else NULL end as [Bounced]
								,sn.EventDate as [Send Timestamp],'TRUE' as Send
					from [_Sent] sn
								inner join [_Job] j on sn.TriggererSendDefinitionObjectID = j.TriggererSendDefinitionObjectID
								inner join [_Subscribers] s on sn.SubscriberKey = s.SubscriberKey
								inner join [Experienced Nudge Group] cm on sn.SubscriberKey = cm.[Deloitte Email Address]
								left join [_Open] o on sn.SubscriberKey = o.SubscriberKey and sn.TriggererSendDefinitionObjectID = o.TriggererSendDefinitionObjectID and o.IsUnique = 1
								left join [_Click] cl on sn.SubscriberKey = cl.SubscriberKey and sn.TriggererSendDefinitionObjectID = cl.TriggererSendDefinitionObjectID and cl.IsUnique = 1
								left join [_bounce] bnc on sn.SubscriberKey = bnc.SubscriberKey and sn.TriggererSendDefinitionObjectID = bnc.TriggererSendDefinitionObjectID
					where

								(
					  				sn.EventDate between dateadd(d,-7,getdate()) and getdate()
								)

								UNION

					Select
								 j.TriggererSendDefinitionObjectID
								,j.EmailName as [Email Name]
								,j.DeliveredTime as [Delivered Timestamp]
								,s.EmailAddress
								,s.SubscriberKey
								,'TRUE' as [Delivered]
								,'TRUE' [Open]
								, o.EventDate as [Open Timestamp]
								,CASE
										When cl.EventDate <>'' then 'TRUE' Else NULL end  as Click
								,cl.EventDate as [Click Timestamp]
								,bnc.EventDate as [Bounced Timestamp]
								,'TRUE' as [Bounced]
								,sn.EventDate as [Send Timestamp]
								,CASE
										When sn.EventDate <> '' then 'TRUE' Else NULL end as Send
					from [_bounce] bnc
								inner join [_Job] j on bnc.TriggererSendDefinitionObjectID = j.TriggererSendDefinitionObjectID
								inner join [_Subscribers] s on bnc.SubscriberKey = s.SubscriberKey
								inner join [Experienced Nudge Group] cm on bnc.SubscriberKey = cm.[Deloitte Email Address]
								left join [_Open] o on bnc.SubscriberKey = o.SubscriberKey and bnc.TriggererSendDefinitionObjectID = o.TriggererSendDefinitionObjectID and o.IsUnique = 1
								left join [_Click] cl on bnc.SubscriberKey = cl.SubscriberKey and bnc.TriggererSendDefinitionObjectID = cl.TriggererSendDefinitionObjectID and cl.IsUnique = 1
								left join [_Sent] sn on bnc.SubscriberKey = sn.SubscriberKey and bnc.TriggererSendDefinitionObjectID = sn.TriggererSendDefinitionObjectID
					where
								(
					  			bnc.EventDate between dateadd(d,-7,getdate()) and getdate()
								)

			) as t
