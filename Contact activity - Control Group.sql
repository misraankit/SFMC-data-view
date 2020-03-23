select
		 t.JobID
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
							 j.JobID
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
							inner join [_Job] j on j.JobID = cl.JobID
							inner join [_Subscribers] s on cl.SubscriberKey = s.SubscriberKey
							inner join [Control Group] cm on cl.SubscriberKey = cm.EmailAddress
							left join [_Sent] sn on cl.SubscriberKey = sn.SubscriberKey and cl.JobID = sn.JobID
							left join [_Open] o on cl.SubscriberKey = o.SubscriberKey and cl.JobID = o.JobID and o.IsUnique = 1
							left join [_bounce] bnc on cl.SubscriberKey = bnc.SubscriberKey and cl.JobID = bnc.JobID and bnc.IsUnique = 1
					where
					 		cl.IsUnique=1 and
								(
					  			cl.EventDate between dateadd(d,-1,getdate()) and getdate()
								)

							UNION

					Select
					  	 j.JobID
							 ,j.EmailName as [Email Name]
							 ,j.DeliveredTime as [Delivered Timestamp]
							 , s.EmailAddress
							 , s.SubscriberKey
							 ,'TRUE' as [Delivered]
							 ,'TRUE' as [Open]
							 , o.EventDate as [Open Timestamp]
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
							 inner join [_Job] j on o.JobID = j.JobID
							 inner join [_Subscribers] s on o.SubscriberKey = s.SubscriberKey
							 inner join [Control Group] cm on o.SubscriberKey = cm.EmailAddress
							 left join [_Sent] sn on o.SubscriberKey = sn.SubscriberKey and o.JobID = sn.JobID
							 left join [_Click] cl on o.SubscriberKey = cl.SubscriberKey and o.JobID = cl.JobID and cl.IsUnique = 1
							 left join [_bounce] bnc on o.SubscriberKey = bnc.SubscriberKey and o.JobID = bnc.JobID
					where
							o.IsUnique = 1 and
								(
					  				o.EventDate between dateadd(d,-1,getdate()) and getdate()
								)

								UNION

					Select
								 j.JobID
								,j.EmailName as [Email Name]
								,j.DeliveredTime as [Delivered Timestamp]
								,s.EmailAddress, s.SubscriberKey
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
								inner join [_Job] j
					on sn.JobID = j.JobID
								inner join [_Subscribers] s
					on sn.SubscriberKey = s.SubscriberKey
								inner join [Control Group] cm on sn.SubscriberKey = cm.EmailAddress
								left join [_Open] o on sn.SubscriberKey = o.SubscriberKey and sn.JobID = o.JobID and o.IsUnique = 1
								left join [_Click] cl on sn.SubscriberKey = cl.SubscriberKey and sn.JobID = cl.JobID and cl.IsUnique = 1
								left join [_bounce] bnc on sn.SubscriberKey = bnc.SubscriberKey and sn.JobID = bnc.JobID
					where

								(
					  				sn.EventDate between dateadd(d,-1,getdate()) and getdate()
								)

								UNION

					Select
								 j.JobID
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
								inner join [_Job] j on bnc.JobID = j.JobID
								inner join [_Subscribers] s on bnc.SubscriberKey = s.SubscriberKey
								inner join [Control Group] cm on bnc.SubscriberKey = cm.EmailAddress
								left join [_Open] o on bnc.SubscriberKey = o.SubscriberKey and bnc.JobID = o.JobID and o.IsUnique = 1
								left join [_Click] cl on bnc.SubscriberKey = cl.SubscriberKey and bnc.JobID = cl.JobID and cl.IsUnique = 1
								left join [_Sent] sn on bnc.SubscriberKey = sn.SubscriberKey and bnc.JobID = sn.JobID
					where
								(
					  			bnc.EventDate between dateadd(d,-1,getdate()) and getdate()
								)

			) as t
