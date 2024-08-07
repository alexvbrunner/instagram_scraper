import requests

class InstagramAPI:
    def __init__(self, cookie):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Your User Agent',
            'Cookie': cookie
        })

    def get_user_profile(self, username):
        query = """
        query {
          user(username: "%s") {
            id
            username
            full_name
            biography
            profile_pic_url
            is_private
          }
        }
        """ % username

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_feed(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_web_feed_timeline {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                  video_url
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_stories(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_owner_to_timeline_media {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                  video_url
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_hashtag_posts(self, hashtag):
        query = """
        query {
          hashtag(name: "%s") {
            edge_hashtag_to_media {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                  video_url
                }
              }
            }
          }
        }
        """ % hashtag

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_location_posts(self, location_id):
        query = """
        query {
          location(id: "%s") {
            edge_location_to_media {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                  video_url
                }
              }
            }
          }
        }
        """ % location_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_followers(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_followed_by {
              count
              edges {
                node {
                  id
                  username
                  full_name
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_following(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_follow {
              count
              edges {
                node {
                  id
                  username
                  full_name
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_post_comments(self, post_id):
        query = """
        query {
          media(id: "%s") {
            edge_media_to_comment {
              count
              edges {
                node {
                  id
                  text
                  created_at
                  owner {
                    username
                  }
                }
              }
            }
          }
        }
        """ % post_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_post_likes(self, post_id):
        query = """
        query {
          media(id: "%s") {
            edge_liked_by {
              count
              edges {
                node {
                  id
                  username
                }
              }
            }
          }
        }
        """ % post_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_tagged_posts(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_user_to_tagged_media {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_igtv(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_igtv_media {
              edges {
                node {
                  id
                  shortcode
                  title
                  cover_media {
                    display_url
                  }
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_highlights(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_highlight_reels {
              edges {
                node {
                  id
                  title
                  cover_media {
                    display_url
                  }
                  highlight_items {
                    id
                    media {
                      display_url
                    }
                  }
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()

    def get_user_media(self, user_id):
        query = """
        query {
          user(id: "%s") {
            edge_owner_to_timeline_media {
              edges {
                node {
                  id
                  shortcode
                  display_url
                  is_video
                }
              }
            }
          }
        }
        """ % user_id

        response = self.session.post('https://www.instagram.com/graphql/query/', data={'query': query})
        return response.json()