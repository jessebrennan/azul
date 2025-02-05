Author

- [ ] PR title references issue
- [ ] Title of main commit references issue
- [ ] PR is linked to Zenhub issue

Author (reindex)

- [ ] Added `r` tag to commit title                         <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label to PR                           <sub>or this PR does not require reindexing</sub>

Author (freebies & chains)

- [ ] Freebies are blocked on this PR                       <sub>or there are no freebies in this PR</sub>
- [ ] Freebies are referenced in commit titles              <sub>or there are no freebies in this PR</sub>
- [ ] This PR is blocked by previous PR in the chain        <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chain` label to the blocking PR                <sub>or this PR is not chained to another PR</sub>

Author (upgrading)

- [ ] Documented upgrading of deployments in UPGRADING.rst  <sub>or this PR does not require upgrading</sub>
- [ ] Added `u` tag to commit title                         <sub>or this PR does not require upgrading</sub>
- [ ] Added `upgrade` label to PR                           <sub>or this PR does not require upgrading</sub>

Author (requirements, before every review)

- [ ] Ran `make requirements_update`                        <sub>or this PR leaves requirements*.txt, common.mk and Makefile untouched</sub>
- [ ] Added `R` tag to commit title                         <sub>or this PR leaves requirements*.txt untouched</sub>
- [ ] Added `reqs` label to PR                              <sub>or this PR leaves requirements*.txt untouched</sub>

Author (before every review)

- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not touch functionality that could break the IT</sub>
- [ ] Rebased branch on `develop`, squashed old fixups

Primary reviewer (after approval)

- [ ] Commented on demo expectations                        <sub>or labelled PR as `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to Approved column
- [ ] Assigned PR to an operator

Operator (before pushing merge the commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Checked that Demo expectations are clear              <sub>or PR is labeled as `no demo`</sub>
- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Pushed PR branch to Github
- [ ] Branch pushed to Gitlab and build passes in `sandbox` <sub>or added `no sandbox` label</sub>
- [ ] Started reindex in `sandbox`                          <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `sandbox`                     <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved linked issue to Merged column
- [ ] Pushed merge commit to Github

Operator (after pushing the merge commit)

- [ ] Moved freebies to Merged column                       <sub>or there are no freebies in this PR</sub> 
- [ ] Shortened the PR chain                                <sub>or this PR is not the base of another PR</sub>
- [ ] Verified that `N reviews` labelling is accurate
- [ ] Pushed merge commit to Gitlab                         <sub>or this changes can be pushed later, together with another PR</sub>
- [ ] Deleted PR branch from Github and Gitlab

Operator (reindex) 

- [ ] Started reindex in `dev`                              <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Checked for failures in `dev`                         <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Started reindex in `prod`                             <sub>or this PR does not require reindexing `prod`</sub>
- [ ] Checked for failures in `prod`                        <sub>or this PR does not require reindexing `prod`</sub>

Operator

- [ ] Unassigned PR
