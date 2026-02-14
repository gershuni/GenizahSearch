import resend

resend.api_key = "re_hJ7G6dcR_FBRYPn7VJ1gkUzYsgWtBaCe9"

r = resend.Emails.send({
  "from": "onboarding@resend.dev",
  "to": "gershuni@gmail.com",
  "subject": "Hello World",
  "html": "<p>Congrats on sending your <strong>first email</strong>!</p>"
})
